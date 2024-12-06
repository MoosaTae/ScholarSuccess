from pyspark.sql.functions import (
    col,
    explode_outer,
    to_date,
    array,
    struct,
    lit,
    when,
    to_json
)
from pyspark.sql.types import IntegerType


class ScopusScraperTransformer:
    def apply_all_transforms(self, df):
        """
        Apply transformations to scraped Scopus data to match the original schema
        """
        # Transform the data to match the original schema
        df = df.select(
            # Core fields that map directly
            col("prism:doi").alias("doi"),
            col("dc:title").alias("title"),
            # Now we have abstract
            col("abstract").alias("abstract"),
            col("subtypeDescription").alias("document_type"),
            col("prism:aggregationType").alias("source_type"),
            to_date(col("prism:coverDate")).alias("publication_date"),
            col("prism:publicationName").alias("source_title"),
            # No direct publisher info in scraped data
            lit(None).alias("publisher"),
            # Now we have author keywords
            when(col("author-keyword").isNotNull(), 
                 to_json(col("author-keyword")))
            .otherwise(lit("[]"))
            .alias("author_keywords"),
            # No subject areas in scraped data, create empty struct to match schema
            struct(
                lit(None).alias("@code"),
                lit(None).alias("@abbrev"),
                lit(None).alias("$")
            ).alias("subject_area"),
            col("citedby-count").cast(IntegerType()).alias("cited_by"),
            when(col("openaccessFlag") == True, 1)
            .otherwise(0)
            .cast(IntegerType())
            .alias("open_access"),
            # No language info in scraped data
            lit(None).alias("language"),
            # No status state in scraped data
            lit(None).alias("status_state"),
            # No reference count in scraped data
            lit(None).cast(IntegerType()).alias("ref_count"),
            # No delivered date in scraped data
            lit(None).alias("delivered_date"),
            # Handle affiliation array
            to_json(col("affiliation")).alias("affiliation"),
            # Handle author details
            explode_outer(col("author")).alias("author_details"),
            # No funding details in scraped data
            lit(None).alias("funding_details")
        )
        
        # Handle remaining fields
        df = df.select(
            "*",
            # Extract subject area details (will be null from struct above)
            col("subject_area.@code").alias("subject_code"),
            col("subject_area.@abbrev").alias("subject_abbrev"),
            col("subject_area.$").alias("subject_name"),
            # Extract author details
            col("author_details.preferred-name.ce:given-name").alias("author_given_name"),
            col("author_details.preferred-name.ce:surname").alias("author_surname"),
            col("author_details.author-url").alias("author_url"),
            # Store author affiliation as is
            col("author_details.affiliation").alias("author_affiliation")
        )
        
        # Apply the same conference proceeding filter as the original transformer
        conference_and_proceed = (
            (col("document_type") == "Conference Paper")
            & (col("source_type") == "Conference Proceeding")
        )
        
        not_conference_and_not_proceed = (
            (col("document_type") != "Conference Paper")
            & (col("source_type") != "Conference Proceeding")
        )
        
        return df.filter(col("doi").isNotNull() & (conference_and_proceed | not_conference_and_not_proceed))