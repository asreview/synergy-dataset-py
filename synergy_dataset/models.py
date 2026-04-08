"""Pydantic models for OpenAlex Work objects.

These models are derived from the OpenAlex OpenAPI schema archived on
https://web.archive.org/web/20260408131522/https://developers.openalex.org/api-reference/openapi.json
Validation is opt-out: Dataset.iter() validates by default
and can be disabled with validate=False for performance-sensitive use.
"""

from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict


class _Base(BaseModel):
    model_config = ConfigDict(extra="allow")


class DehydratedAuthor(_Base):
    id: str | None = None
    display_name: str | None = None
    orcid: str | None = None


class DehydratedInstitution(_Base):
    id: str | None = None
    display_name: str | None = None
    ror: str | None = None
    country_code: str | None = None
    type: str | None = None
    lineage: list[str] = []


class DehydratedSource(_Base):
    id: str | None = None
    display_name: str | None = None
    issn_l: str | None = None
    issn: list[str] | None = None
    is_oa: bool | None = None
    is_in_doaj: bool | None = None
    is_core: bool | None = None
    host_organization: str | None = None
    host_organization_name: str | None = None
    host_organization_lineage: list[str] = []
    type: str | None = None


class Location(_Base):
    is_oa: bool | None = None
    landing_page_url: str | None = None
    pdf_url: str | None = None
    source: DehydratedSource | None = None
    license: str | None = None
    license_id: str | None = None
    version: str | None = None
    is_accepted: bool | None = None
    is_published: bool | None = None


class OpenAccess(_Base):
    is_oa: bool | None = None
    oa_status: str | None = None
    oa_url: str | None = None
    any_repository_has_fulltext: bool | None = None


class Authorship(_Base):
    author_position: str | None = None
    author: DehydratedAuthor | None = None
    institutions: list[DehydratedInstitution] = []
    countries: list[str] = []
    is_corresponding: bool | None = None
    raw_author_name: str | None = None
    raw_affiliation_strings: list[str] = []


class TopicHierarchyEntry(_Base):
    id: str | None = None
    display_name: str | None = None


class WorkTopic(_Base):
    id: str | None = None
    display_name: str | None = None
    score: float | None = None
    subfield: TopicHierarchyEntry | None = None
    field: TopicHierarchyEntry | None = None
    domain: TopicHierarchyEntry | None = None


class WorkKeyword(_Base):
    id: str | None = None
    display_name: str | None = None
    score: float | None = None


class DehydratedFunder(_Base):
    id: str | None = None
    display_name: str | None = None
    ror: str | None = None


class Award(_Base):
    id: str | None = None
    display_name: str | None = None
    funder_award_id: str | None = None
    funder_id: str | None = None
    funder_display_name: str | None = None
    doi: str | None = None


class WorkIds(_Base):
    openalex: str | None = None
    doi: str | None = None
    mag: int | None = None
    pmid: str | None = None
    pmcid: str | None = None


class Biblio(_Base):
    volume: str | None = None
    issue: str | None = None
    first_page: str | None = None
    last_page: str | None = None


class CitationNormalizedPercentile(_Base):
    value: float | None = None
    is_in_top_1_percent: bool | None = None
    is_in_top_10_percent: bool | None = None


class CitedByPercentileYear(_Base):
    min: int | None = None
    max: int | None = None


class CountsByYear(_Base):
    year: int | None = None
    cited_by_count: int | None = None


class MeshTag(_Base):
    descriptor_ui: str | None = None
    descriptor_name: str | None = None
    qualifier_ui: str | None = None
    qualifier_name: str | None = None
    is_major_topic: bool | None = None


class SustainableDevelopmentGoal(_Base):
    id: str | None = None
    display_name: str | None = None
    score: float | None = None


class HasContent(_Base):
    pdf: bool | None = None
    grobid_xml: bool | None = None


class WorkModel(_Base):
    """Validated representation of an OpenAlex Work object."""

    id: str | None = None
    doi: str | None = None
    title: str | None = None
    display_name: str | None = None
    publication_year: int | None = None
    publication_date: str | None = None
    type: str | None = None
    language: str | None = None
    cited_by_count: int | None = None
    is_retracted: bool | None = None
    is_paratext: bool | None = None
    primary_location: Location | None = None
    locations: list[Location] = []
    best_oa_location: Location | None = None
    open_access: OpenAccess | None = None
    authorships: list[Authorship] = []
    ids: WorkIds | None = None
    biblio: Biblio | None = None
    abstract_inverted_index: dict[str, Any] | None = None
    referenced_works: list[str] = []
    referenced_works_count: int | None = None
    related_works: list[str] = []
    topics: list[WorkTopic] = []
    primary_topic: WorkTopic | None = None
    keywords: list[WorkKeyword] = []
    funders: list[DehydratedFunder] = []
    awards: list[Award] = []
    fwci: float | None = None
    citation_normalized_percentile: CitationNormalizedPercentile | None = None
    cited_by_percentile_year: CitedByPercentileYear | None = None
    counts_by_year: list[CountsByYear] = []
    sustainable_development_goals: list[SustainableDevelopmentGoal] = []
    mesh: list[MeshTag] = []
    indexed_in: list[str] = []
    has_content: HasContent | None = None
    content_url: str | None = None
    created_date: str | None = None
    updated_date: str | None = None

    # Non-standard fields added by the SYNERGY dataset maintainers.
    # These are not part of the OpenAlex schema.
    abstract_inverted_index_cleaned: dict[str, Any] | None = None
    language_fasttext: str | None = None
