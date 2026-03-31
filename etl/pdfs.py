import modal

import etl.shared

image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("langchain~=0.0.98", "pymongo[srv]==3.11", "arxiv~=2.1", "pypdf==3.8.1")
    .add_local_python_source("etl", "docstore", "utils")
)

app = modal.App(
    name="etl-pdfs",
    image=image,
    secrets=[
        modal.Secret.from_name("mongodb-fsdl"),
    ],
)


@app.local_entrypoint()
def main(json_path="data/llm-papers.json", collection=None, db=None):
    """Calls the ETL pipeline using a JSON file with PDF metadata.

    modal run etl/pdfs.py --json-path /path/to/json
    """
    import json
    from pathlib import Path

    json_path = Path(json_path).resolve()

    if not json_path.exists():
        print(f"{json_path} not found, writing to it from the database.")
        paper_data = fetch_papers.remote()
        paper_data_json = json.dumps(paper_data, indent=2)
        with open(json_path, "w") as f:
            f.write(paper_data_json)

    with open(json_path) as f:
        paper_data = json.load(f)

    paper_data = get_pdf_url.map(paper_data, return_exceptions=True)

    documents = etl.shared.unchunk(extract_pdf.map(paper_data, return_exceptions=True))

    chunked_documents = etl.shared.chunk_into(documents, 10)
    list(
        etl.shared.add_to_document_db.map(
            chunked_documents, kwargs={"db": db, "collection": collection}
        )
    )


@app.function(
    image=image,
    # we can automatically retry execution of Modal functions on failure
    # -- this retry policy does exponential backoff
    retries=modal.Retries(backoff_coefficient=2.0, initial_delay=5.0, max_retries=3),
    # 限制并发容器数，避免触发 arxiv 的限速
    max_containers=20,
)
def extract_pdf(paper_data):
    """Extracts the text from a PDF and adds metadata."""
    import logging

    import arxiv

    from langchain.document_loaders import PyPDFLoader

    import random
    import time

    pdf_url = paper_data.get("pdf_url")
    title = paper_data.get("title", pdf_url)
    if pdf_url is None:
        print(f"[extract_pdf] 跳过（无 PDF URL）: {paper_data.get('title', paper_data.get('url', '未知'))}")
        return []

    print(f"[extract_pdf] 开始下载: {title[:60]} <- {pdf_url}")
    logger = logging.getLogger("pypdf")
    logger.setLevel(logging.ERROR)

    loader = PyPDFLoader(pdf_url)

    try:
        documents = loader.load_and_split()
    except Exception as e:
        err_str = str(e)
        # 404/403 是永久性错误，直接跳过不重试
        if "404" in err_str or "403" in err_str:
            print(f"[extract_pdf] ✗ 跳过（{err_str[:40]}）: {title[:60]}")
            return []
        print(f"[extract_pdf] ✗ 下载失败: {title[:60]} ({e})")
        raise  # 其他错误抛出，触发 Modal 重试

    documents = [document.dict() for document in documents]
    for document in documents:  # rename page_content to text, handle non-unicode data
        document["text"] = (
            document["page_content"].encode("utf-8", errors="replace").decode()
        )
        document.pop("page_content")

    if "arxiv" in pdf_url:
        arxiv_id = extract_arxiv_id_from_url(pdf_url)
        # create an arXiV database client with a 5 second delay between requests
        client = arxiv.Client(page_size=1, delay_seconds=5, num_retries=5)
        # describe a search of arXiV's database
        search_query = arxiv.Search(id_list=[arxiv_id], max_results=1)
        try:
            # execute the search with the client and get the first result
            result = next(client.results(search_query))
            metadata = {
                "arxiv_id": arxiv_id,
                "title": result.title,
                "date": result.updated,
            }
        except ConnectionResetError as e:
            raise Exception("Triggered request limit on arxiv.org, retrying") from e
        except arxiv.HTTPError as e:
            if e.status in (403, 404):
                # 永久性错误，降级不重试
                print(f"[extract_pdf] arxiv API HTTP {e.status}，降级使用原始标题: {title[:60]}")
                metadata = {"arxiv_id": arxiv_id, "title": paper_data.get("title", arxiv_id)}
            else:
                # 其他 HTTP 错误（如 301）也降级
                print(f"[extract_pdf] arxiv API 元数据获取失败（{e}），降级使用原始标题")
                metadata = {"arxiv_id": arxiv_id, "title": paper_data.get("title", arxiv_id)}
        except StopIteration:
            print(f"[extract_pdf] arxiv API 返回空结果，降级使用原始标题: {title[:60]}")
            metadata = {"arxiv_id": arxiv_id, "title": paper_data.get("title", arxiv_id)}
    else:
        metadata = {"title": paper_data.get("title")}

    documents = annotate_endmatter(documents)

    for document in documents:
        document["metadata"]["source"] = paper_data.get("url", pdf_url)
        document["metadata"] |= metadata
        title, page = (
            document["metadata"]["title"],
            document["metadata"]["page"],
        )
        if title:
            document["metadata"]["full-title"] = f"{title} - p{page}"

    documents = etl.shared.enrich_metadata(documents)

    print(f"[extract_pdf] ✓ 完成: {title[:60]}，共 {len(documents)} 页")
    return documents


@app.function()
def fetch_papers(collection_name="all-content"):
    """Fetches papers from the LLM Lit Review, https://tfs.ai/llm-lit-review."""
    import docstore

    client = docstore.connect()

    collection = client.get_database("llm-lit-review").get_collection(collection_name)

    # Query to retrieve documents with the "PDF?" field set to true
    query = {"properties.PDF?.checkbox": {"$exists": True, "$eq": True}}

    # Projection to include the "Name", "url", and "Tags" fields
    projection = {
        "properties.Name.title.plain_text": 1,
        "properties.Link.url": 1,
        "properties.Tags.multi_select.name": 1,
    }

    # Fetch documents matching the query and projection
    documents = list(collection.find(query, projection))
    assert documents

    papers = []
    for doc in documents:
        paper = {}
        paper["title"] = doc["properties"]["Name"]["title"][0]["plain_text"]
        paper["url"] = doc["properties"]["Link"]["url"]
        paper["tags"] = [
            tag["name"]
            for tag in doc.get("properties", {}).get("Tags", {}).get("multi_select", [])
        ]
        papers.append(paper)

    assert papers

    return papers


@app.function()
def get_pdf_url(paper_data):
    """Attempts to extract a PDF URL from a paper's URL."""
    url = paper_data["url"]
    title = paper_data.get("title", url)
    if url.strip("#/").endswith(".pdf"):
        pdf_url = url
    elif "arxiv.org" in url:
        arxiv_id = extract_arxiv_id_from_url(url)
        pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    elif "aclanthology.org" in url:
        pdf_url = url.strip("/")
        url += ".pdf"
    else:
        pdf_url = None
    paper_data["pdf_url"] = pdf_url
    print(f"[get_pdf_url] {'✓' if pdf_url else '✗'} {title[:60]} -> {pdf_url or '无法获取 PDF URL'}")

    return paper_data


def annotate_endmatter(pages, min_pages=6):
    """Heuristic for detecting reference sections."""
    out, after_references = [], False
    for idx, page in enumerate(pages):
        content = page["text"].lower()
        if idx >= min_pages and ("references" in content or "bibliography" in content):
            after_references = True
        page["metadata"]["is_endmatter"] = after_references
        out.append(page)
    return out


def extract_arxiv_id_from_url(url):
    import re

    # pattern = r"(?:arxiv\.org/abs/|arxiv\.org/pdf/)(\d{4}\.\d{4,5}(?:v\d+)?)"
    match_arxiv_url = r"(?:arxiv\.org/abs/|arxiv\.org/pdf/)"
    match_id = r"(\d{4}\.\d{4,5}(?:v\d+)?)"  # 4 digits, a dot, and 4 or 5 digits
    optional_version = r"(?:v\d+)?"

    pattern = match_arxiv_url + match_id + optional_version

    match = re.search(pattern, url)
    if match:
        return match.group(1)
    else:
        return None
