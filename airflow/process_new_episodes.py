import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime
from rainbowbatch.pipeline.citation_extractor import download_citations
from rainbowbatch.pipeline.citation_extractor import reprocess_citation_episodes

from rainbowbatch.pipeline.episode_details_downloader import download_episode_details
from rainbowbatch.pipeline.merge import merge_records
#from rainbowbatch.pipeline.stamp_episode_listing import stamp_episode_listing
#from rainbowbatch.pipeline.stamp_template import stamp_templates
from rainbowbatch.pipeline.title_download import download_titles

def unprocessed_episodes_exist():
    # TODO: Check if there are episode ids that are not present in final.
    return True  # Or False to skip

def merge_wrapper():
    merge_records()
    # This is a bit clunky, but we need to make sure that we propagate information
    # through the date listing so that citations are properly matched. It's probably
    # usually un-needed, but better to have it then not.
    if reprocess_citation_episodes():
        logging.info("Doing second merge due to recalculated citations.")
        merge_records()  # In case any citations have new associations.

with DAG(
    "process_new_episodes",
    start_date=datetime(2025, 6, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:
    unprocessed_episodes_exist = ShortCircuitOperator(
        task_id="unprocessed_episodes_exist",
        python_callable=unprocessed_episodes_exist,
        dag=dag,
    )

    download_titles_task = PythonOperator(
        task_id='download_titles',
        python_callable=download_titles,
    )

    download_episode_details_task = PythonOperator(
        task_id='download_episode_details',
        python_callable=download_episode_details,
    )

    download_citations_task = PythonOperator(
        task_id='download_citations',
        python_callable=download_citations,
    )

    merge_records_task = PythonOperator(
        task_id='merge_episode_records',
        python_callable=merge_wrapper,
    )

    #stamp_templates_task = PythonOperator(
    #    task_id='stamp_templates',
    #    python_callable=stamp_templates,
    #)

    #stamp_listing_task = PythonOperator(
    #    task_id='stamp_episode_listing',
    #    python_callable=stamp_episode_listing,
    #)

    download_titles_task >> unprocessed_episodes_exist
    unprocessed_episodes_exist >> download_episode_details_task >> download_citations_task

    [
        download_citations_task,
        download_citations_task,
        download_episode_details_task,
    ] >> merge_records_task

    # merge_records_task >> stamp_templates_task >> stamp_listing_task
