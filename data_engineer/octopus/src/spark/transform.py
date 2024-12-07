from pyspark.sql.functions import (
    col,
    concat_ws,
    explode_outer,
    to_date,
)
from pyspark.sql.types import IntegerType


class ScopusTransformer:
    def apply_all_transforms(self, df):
        """Apply all transformations in sequence"""
        df = df.select(
            col("abstracts-retrieval-response.coredata.prism:doi").alias("doi"),
            col("abstracts-retrieval-response.coredata.dc:title").alias("title"),
            col("abstracts-retrieval-response.coredata.dc:description").alias(
                "abstract"
            ),
            col("abstracts-retrieval-response.coredata.subtypeDescription").alias(
                "document_type"
            ),
            col("abstracts-retrieval-response.coredata.prism:aggregationType").alias(
                "source_type"
            ),
            to_date(col("abstracts-retrieval-response.coredata.prism:coverDate")).alias(
                "publication_date"
            ),
            col("abstracts-retrieval-response.coredata.prism:publicationName").alias(
                "source_title"
            ),
            col("abstracts-retrieval-response.coredata.dc:publisher").alias(
                "publisher"
            ),
            col("abstracts-retrieval-response.authkeywords.author-keyword").alias(
                "author_keywords"
            ),
            explode_outer(
                col("abstracts-retrieval-response.subject-areas.subject-area")
            ).alias("subject_area"),
            col("abstracts-retrieval-response.coredata.citedby-count")
            .cast(IntegerType())
            .alias("cited_by"),
            col("abstracts-retrieval-response.coredata.openaccess")
            .cast(IntegerType())
            .alias("open_access"),
            col("abstracts-retrieval-response.language.@xml:lang").alias("language"),
            col(
                "abstracts-retrieval-response.item.ait:process-info.ait:status.@state"
            ).alias("status_state"),
            col(
                "abstracts-retrieval-response.item.bibrecord.tail.bibliography.@refcount"
            )
            .cast(IntegerType())
            .alias("ref_count"),
            concat_ws(
                "-",
                col(
                    "abstracts-retrieval-response.item.ait:process-info.ait:date-delivered.@year"
                ),
                col(
                    "abstracts-retrieval-response.item.ait:process-info.ait:date-delivered.@month"
                ),
                col(
                    "abstracts-retrieval-response.item.ait:process-info.ait:date-delivered.@day"
                ),
            ).alias("delivered_date"),
            col("abstracts-retrieval-response.affiliation").alias("affiliation"),
            explode_outer(col("abstracts-retrieval-response.authors.author")).alias(
                "author_details"
            ),
            col("abstracts-retrieval-response.item.xocs:meta.xocs:funding-list").alias(
                "funding_details"
            ),
        ).select(
            "*",
            col("subject_area.@code").alias("subject_code"),
            col("subject_area.@abbrev").alias("subject_abbrev"),
            col("subject_area.$").alias("subject_name"),
            col("author_details.preferred-name.ce:given-name").alias(
                "author_given_name"
            ),
            col("author_details.preferred-name.ce:surname").alias("author_surname"),
            col("author_details.author-url").alias("author_url"),
            col("author_details.affiliation").alias("author_affiliation"),
        )
        
        """
        Apply the condition that we need only 
        (confer, proceed) and (not confer, not proceed) 
        """
        conference_and_proceed = (
            (col("document_type") == "Conference Paper")
            & (col("source_type") == "Conference Proceeding")
        )
        
        not_conference_and_not_proceed = (
            (col("document_type") != "Conference Paper")
            & (col("source_type") != "Conference Proceeding")
        )
        
        return df.filter(col("doi").isNotNull() & (conference_and_proceed | not_conference_and_not_proceed))
