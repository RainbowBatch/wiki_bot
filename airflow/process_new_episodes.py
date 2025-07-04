import logging
import rainbowbatch.kfio as kfio

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from rainbowbatch.pipeline.citation_extractor import download_citations
from rainbowbatch.pipeline.citation_extractor import reprocess_citation_episodes

from rainbowbatch.remap.episode_number_util import extract_episode_number
from rainbowbatch.git import check_git_branch
from rainbowbatch.pipeline.download_audio_files import download_audio_files
from rainbowbatch.pipeline.episode_details_downloader import download_episode_details
from rainbowbatch.pipeline.external.spotify_downloader import download_spotify_details
from rainbowbatch.pipeline.external.twitch_downloader import download_twitch_details
from rainbowbatch.pipeline.merge import merge_records
from rainbowbatch.pipeline.stamp.stamp_episode_listing import stamp_episode_listing
from rainbowbatch.pipeline.stamp.stamp_template import stamp_templates
from rainbowbatch.pipeline.title_download import download_titles

def unprocessed_episodes_exist():
    title_table = kfio.load('data/titles.json')
    final_table = kfio.load('data/final.json')

    title_eps = set(title_table['title'].apply(extract_episode_number))
    final_eps = set(final_table['episode_number'])

    missing_eps = title_eps - final_eps

    if missing_eps:
        print(f"Unprocessed episodes: {sorted(missing_eps)}")
        return True
    return False


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
    check_unprocessed_task = ShortCircuitOperator(
        task_id="unprocessed_episodes_exist",
        python_callable=unprocessed_episodes_exist,
        dag=dag,
    )

    on_branch_bot_raw = ShortCircuitOperator(
        task_id="on_branch_bot_raw",
        python_callable=lambda: check_git_branch("bot_raw"),
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

    download_spotify_details_task = PythonOperator(
        task_id='download_spotify_details',
        python_callable=download_spotify_details
    )

    download_twitch_details_task = PythonOperator(
        task_id='download_twitch_details',
        python_callable=download_twitch_details
    )

    # If these downloaders break, don't block the merge.
    optional_downloaders_task = EmptyOperator(
        task_id='optional_downloaders',
        trigger_rule=TriggerRule.ALL_DONE
    )

    merge_records_task = PythonOperator(
        task_id='merge_episode_records',
        python_callable=merge_wrapper,
    )

    download_audio_files_task = PythonOperator(
        task_id='download_audio_files',
        python_callable=download_audio_files
    )

    stamp_templates_task = PythonOperator(
        task_id='stamp_templates',
        python_callable=stamp_templates,
    )

    stamp_listing_task = PythonOperator(
        task_id='stamp_episode_listing',
        python_callable=stamp_episode_listing,
    )

    download_titles_task >> check_unprocessed_task
    check_unprocessed_task >> download_episode_details_task >> download_citations_task

    # External third party services which are flaky.
    check_unprocessed_task >> download_spotify_details_task
    check_unprocessed_task >> download_twitch_details_task
    [
        download_spotify_details_task,
        download_twitch_details_task,
    ] >> optional_downloaders_task

    # Merge everything into final.json
    [
        download_titles_task,
        download_citations_task,
        download_episode_details_task,
        optional_downloaders_task,
    ] >> merge_records_task

    merge_records_task >> download_audio_files_task
    merge_records_task >> on_branch_bot_raw >> stamp_templates_task >> stamp_listing_task