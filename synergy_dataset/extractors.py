"""Field extractors for OpenAlex Work objects.

Each entry in ``WORK_EXTRACTORS`` maps a field name to a function that
accepts a ``pyalex.Work`` (dict-like) and returns the extracted value. Pass
field names as the ``vars`` argument to ``Dataset.to_dict()`` /
``Dataset.to_frame()``.

Use ``vars="extended"`` to include every field defined here.
"""

# Fields included by default when no ``vars`` argument is given.
DEFAULT_VARS = ["title", "abstract"]


def _clean_str(value):
    if isinstance(value, str):
        return value.replace("\n", " ").replace("\r", "")
    return value


def _reconstruct_abstract(abstract_inverted_index):
    """Reconstruct plain-text abstract from an inverted index.

    Args:
        abstract_inverted_index (dict): Mapping of word -> list of positions.

    Returns:
        str or None: Reconstructed abstract, or None if index is empty/None.
    """
    if not abstract_inverted_index:
        return None
    positions = []
    for word, pos_list in abstract_inverted_index.items():
        for pos in pos_list:
            positions.append((pos, word))
    positions.sort(key=lambda x: x[0])
    return " ".join(word for _, word in positions)


def _get_authorships(work):
    result = []
    for a in work.get("authorships") or []:
        author = a.get("author") or {}
        result.append(
            {
                "author_id": author.get("id"),
                "author_name": author.get("display_name"),
                "author_position": a.get("author_position"),
                "is_corresponding": a.get("is_corresponding"),
                "raw_author_name": a.get("raw_author_name"),
                "institutions": [
                    {
                        "id": i.get("id"),
                        "name": i.get("display_name"),
                        "country_code": i.get("country_code"),
                        "type": i.get("type"),
                    }
                    for i in (a.get("institutions") or [])
                ],
                "countries": a.get("countries"),
            }
        )
    return result


WORK_EXTRACTORS = {
    # --- Core bibliographic ---
    "title": lambda w: _clean_str(w.get("title")),
    "abstract": lambda w: _clean_str(
        _reconstruct_abstract(w.get("abstract_inverted_index_cleaned"))
    ),
    "abstract_original": lambda w: _clean_str(
        _reconstruct_abstract(w.get("abstract_inverted_index"))
    ),
    "publication_year": lambda w: w.get("publication_year"),
    "publication_date": lambda w: w.get("publication_date"),
    "type": lambda w: w.get("type"),
    "language": lambda w: w.get("language"),
    "language_fasttext": lambda w: w.get("language_fasttext"),
    # --- Citation metrics ---
    "cited_by_count": lambda w: w.get("cited_by_count"),
    "referenced_works_count": lambda w: w.get("referenced_works_count"),
    "fwci": lambda w: w.get("fwci"),
    # --- Flags ---
    "is_retracted": lambda w: w.get("is_retracted"),
    "is_paratext": lambda w: w.get("is_paratext"),
    # --- Open access ---
    "is_oa": lambda w: (w.get("open_access") or {}).get("is_oa"),
    "oa_status": lambda w: (w.get("open_access") or {}).get("oa_status"),
    # --- Venue ---
    "journal_name": lambda w: (
        (w.get("primary_location") or {}).get("source") or {}
    ).get("display_name"),
    # --- Authors ---
    "author_names": lambda w: [
        (a.get("author") or {}).get("display_name")
        for a in (w.get("authorships") or [])
        if (a.get("author") or {}).get("display_name")
    ],
    "authorships": _get_authorships,
    # --- Topics ---
    "primary_topic_name": lambda w: (w.get("primary_topic") or {}).get(
        "display_name"
    ),
    "primary_topic_field": lambda w: (
        (w.get("primary_topic") or {}).get("field") or {}
    ).get("display_name"),
    "primary_topic_domain": lambda w: (
        (w.get("primary_topic") or {}).get("domain") or {}
    ).get("display_name"),
    "topics": lambda w: [
        {"id": t.get("id"), "name": t.get("display_name"), "score": t.get("score")}
        for t in (w.get("topics") or [])
    ],
    # --- Keywords / MeSH / SDGs ---
    "keywords": lambda w: [
        k.get("display_name") for k in (w.get("keywords") or [])
    ],
    "mesh": lambda w: [
        {
            "descriptor_name": m.get("descriptor_name"),
            "qualifier_name": m.get("qualifier_name"),
            "is_major_topic": m.get("is_major_topic"),
        }
        for m in (w.get("mesh") or [])
    ],
    "sustainable_development_goals": lambda w: [
        {"id": s.get("id"), "name": s.get("display_name"), "score": s.get("score")}
        for s in (w.get("sustainable_development_goals") or [])
    ],
    # --- Identifiers / indexing ---
    "indexed_in": lambda w: w.get("indexed_in"),
    "referenced_works": lambda w: w.get("referenced_works"),
    "related_works": lambda w: w.get("related_works"),
    # --- Time series ---
    "counts_by_year": lambda w: [
        {"year": c.get("year"), "cited_by_count": c.get("cited_by_count")}
        for c in (w.get("counts_by_year") or [])
    ],
}
