import re
import urllib.request

from bs4 import BeautifulSoup
import urllib.parse

BASE_URL = "https://www.pref.saga.lg.jp"

def get_html(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response:
        return response.read().decode("utf-8")

def main():
    html = get_html("https://www.pref.saga.lg.jp/list00631.html")
    soup = BeautifulSoup(html, "html.parser")
    next_url_template = None
    for a in soup.find_all("a", rel=True):
        rel_vals = a.get("rel", [])
        if "next" in getattr(a, "get", lambda x: "")("class", []) or "next1" in rel_vals:
            href = a.get("href")
            if href and "hpkijilistpagerhandler.ashx" in href:
                next_url_template = urllib.parse.urljoin(BASE_URL, href)
                break

    print("Template:", next_url_template)
    if next_url_template:
        base = re.sub(r"pg=\d+", "pg={pg}", next_url_template)
        html2 = get_html(base.format(pg=2))
        print("Page 2 len:", len(html2))
        print("Page 2 has kiji?", "kiji" in html2)
        s2 = BeautifulSoup(html2, "html.parser")
        print("Page 2 links:", len(s2.find_all("a", href=re.compile("kiji"))))


if __name__ == "__main__":
    main()
