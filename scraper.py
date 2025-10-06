import time
import json
from datetime import datetime, timezone
from dateutil import tz
from typing import List, Dict, Any

import pandas as pd
from tqdm import tqdm
import newspaper
from newspaper import Article, Source

# ========= Config =========
# Portais de exemplo (adicione/remova à vontade)
NEWS_SITES = [
    "https://g1.globo.com",          # G1
    "https://www.bbc.com/portuguese",
    "https://www1.folha.uol.com.br",
    "https://www.terra.com.br",
    "https://oglobo.globo.com",      # pode ter paywall em alguns links
    "https://www.uol.com.br"
]

# Máximo de artigos por site (protótipo)
MAX_PER_SITE = 40

# Pausa entre downloads para evitar ban/overload
SLEEP_BETWEEN = 0.5  # segundos

# Timezone alvo para normalizar datas
TARGET_TZ = "America/Sao_Paulo"
# ==========================


def build_source(url: str, memoize_articles: bool = False, language: str = "pt") -> Source:
    """
    Constrói um Source do newspaper3k. O 'build()' varre as páginas e populará 'articles'.
    """
    src = newspaper.build(
        url,
        memoize_articles=memoize_articles,
        language=language,  # ajuda na tokenização
        fetch_images=False, # mais rápido
        number_threads=4
    )
    return src


def parse_article(article: Article) -> Dict[str, Any]:
    """
    Baixa e extrai conteúdo de um Article do newspaper3k.
    """
    article.download()
    article.parse()

    # Tenta extrair NLP (keywords/summary); pode falhar em alguns casos
    try:
        article.nlp()
        keywords = article.keywords
        summary = article.summary
    except Exception:
        keywords = []
        summary = ""

    authors = article.authors or []
    top_image = getattr(article, "top_image", "") or ""
    movies = getattr(article, "movies", []) or []

    # Publicação: normaliza para timezone desejado
    pub_date = article.publish_date  # datetime ou None
    if isinstance(pub_date, datetime):
        if pub_date.tzinfo is None:
            pub_date = pub_date.replace(tzinfo=timezone.utc)
        pub_date = pub_date.astimezone(tz.gettz(TARGET_TZ))
        pub_date_iso = pub_date.isoformat()
    else:
        pub_date_iso = None

    return {
        "title": article.title or "",
        "url": article.url,
        "text": article.text or "",
        "authors": authors,
        "top_image": top_image,
        "movies": movies,
        "publish_date": pub_date_iso,
        "source_url": article.source_url if hasattr(article, "source_url") else "",
        "keywords": keywords,
        "summary": summary,
        "download_date": datetime.now(tz=tz.gettz(TARGET_TZ)).isoformat()
    }


def scrape_site(url: str, max_items: int) -> List[Dict[str, Any]]:
    """
    Varre um site, pega até max_items artigos e retorna lista de dicionários prontos.
    """
    out: List[Dict[str, Any]] = []
    try:
        src = build_source(url)
    except Exception as e:
        print(f"[WARN] Falha ao buildar {url}: {e}")
        return out

    # Se o site não retornou artigos, encerra
    if not src.articles:
        print(f"[INFO] Sem artigos encontrados em {url}.")
        return out

    # Limita a quantidade (protótipo)
    articles = src.articles[:max_items]

    for art in tqdm(articles, desc=f"Coletando {url}", unit="art"):
        try:
            data = parse_article(art)
            # Ignora itens sem título ou sem texto (muito comuns)
            if not data["title"] and not data["text"]:
                continue
            out.append(data)
            time.sleep(SLEEP_BETWEEN)
        except newspaper.article.ArticleException as ae:
            # Erros comuns de download/parse
            # print(f"[WARN] Erro no artigo {art.url}: {ae}")
            continue
        except Exception as e:
            # Outros erros (redirecionamentos malucos, etc.)
            # print(f"[WARN] Erro inesperado {art.url}: {e}")
            continue

    return out


def save_outputs(rows: List[Dict[str, Any]], base_name: str = "news_out") -> None:
    """
    Salva em CSV e JSONL.
    """
    if not rows:
        print("[INFO] Nada para salvar.")
        return

    # DataFrame para CSV
    df = pd.DataFrame(rows)
    csv_path = f"{base_name}.csv"
    df.to_csv(csv_path, index=False)
    print(f"[OK] CSV salvo em: {csv_path}")

    
    jsonl_path = f"{base_name}.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[OK] JSONL salvo em: {jsonl_path}")


def main():
    all_rows: List[Dict[str, Any]] = []
    for site in NEWS_SITES:
        rows = scrape_site(site, MAX_PER_SITE)
        all_rows.extend(rows)

    
    seen = set()
    deduped = []
    for r in all_rows:
        if r["url"] in seen:
            continue
        seen.add(r["url"])
        deduped.append(r)

    print(f"[INFO] Total coletado: {len(all_rows)} | Após dedup: {len(deduped)}")
    save_outputs(deduped, base_name=f"news_out_{datetime.now().strftime('%Y%m%d_%H%M')}")


if __name__ == "__main__":
    main()
