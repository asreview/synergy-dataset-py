"""Field extractors for OpenAlex Work objects.

Each entry in ``WORK_EXTRACTORS`` maps a field name to a function that
accepts a ``WorkModel`` and returns the extracted value. Pass field names
as the ``vars`` argument to ``Dataset.to_dict()`` / ``Dataset.to_frame()``.

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
    for a in work.authorships or []:
        result.append(
            {
                "author_id": a.author.id if a.author else None,
                "author_name": a.author.display_name if a.author else None,
                "author_position": a.author_position,
                "is_corresponding": a.is_corresponding,
                "raw_author_name": a.raw_author_name,
                "institutions": [
                    {
                        "id": i.id,
                        "name": i.display_name,
                        "country_code": i.country_code,
                        "type": i.type,
                    }
                    for i in (a.institutions or [])
                ],
                "countries": a.countries,
            }
        )
    return result


WORK_EXTRACTORS = {
    # --- Core bibliographic ---
    "title": lambda w: _clean_str(w.title),
    "abstract": lambda w: _clean_str(
        _reconstruct_abstract(w.abstract_inverted_index_cleaned)
    ),
    "abstract_original": lambda w: _clean_str(
        _reconstruct_abstract(w.abstract_inverted_index)
    ),
    "publication_year": lambda w: w.publication_year,
    "publication_date": lambda w: w.publication_date,
    "type": lambda w: w.type,
    "language": lambda w: w.language,
    "language_fasttext": lambda w: w.language_fasttext,
    # --- Citation metrics ---
    "cited_by_count": lambda w: w.cited_by_count,
    "referenced_works_count": lambda w: w.referenced_works_count,
    "fwci": lambda w: w.fwci,
    # --- Flags ---
    "is_retracted": lambda w: w.is_retracted,
    "is_paratext": lambda w: w.is_paratext,
    # --- Open access ---
    "is_oa": lambda w: w.open_access.is_oa if w.open_access else None,
    "oa_status": lambda w: w.open_access.oa_status if w.open_access else None,
    # --- Venue ---
    "journal_name": lambda w: (
        w.primary_location.source.display_name
        if w.primary_location and w.primary_location.source
        else None
    ),
    # --- Authors ---
    "author_names": lambda w: [
        a.author.display_name
        for a in (w.authorships or [])
        if a.author and a.author.display_name
    ],
    "authorships": _get_authorships,
    # --- Topics ---
    "primary_topic_name": lambda w: (
        w.primary_topic.display_name if w.primary_topic else None
    ),
    "primary_topic_field": lambda w: (
        w.primary_topic.field.display_name
        if w.primary_topic and w.primary_topic.field
        else None
    ),
    "primary_topic_domain": lambda w: (
        w.primary_topic.domain.display_name
        if w.primary_topic and w.primary_topic.domain
        else None
    ),
    "topics": lambda w: [
        {"id": t.id, "name": t.display_name, "score": t.score} for t in (w.topics or [])
    ],
    # --- Keywords / MeSH / SDGs ---
    "keywords": lambda w: [k.display_name for k in (w.keywords or [])],
    "mesh": lambda w: [
        {
            "descriptor_name": m.descriptor_name,
            "qualifier_name": m.qualifier_name,
            "is_major_topic": m.is_major_topic,
        }
        for m in (w.mesh or [])
    ],
    "sustainable_development_goals": lambda w: [
        {"id": s.id, "name": s.display_name, "score": s.score}
        for s in (w.sustainable_development_goals or [])
    ],
    # --- Identifiers / indexing ---
    "indexed_in": lambda w: w.indexed_in,
    "referenced_works": lambda w: w.referenced_works,
    "related_works": lambda w: w.related_works,
    # --- Time series ---
    "counts_by_year": lambda w: [
        {"year": c.year, "cited_by_count": c.cited_by_count}
        for c in (w.counts_by_year or [])
    ],
}
