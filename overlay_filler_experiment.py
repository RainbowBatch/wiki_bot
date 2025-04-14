import pandas as pd
import json
from natsort import natsort_keygen
from collections import Counter, OrderedDict

NLP_THRESHOLD_FRACTION = 1 # Only consider people who appear very rarely.

# Load data
raw_overlay = pd.read_json('data/overlay.json')
raw_final = pd.read_json('data/final.json')
nlp_guests = pd.read_json('data/nlp_guests.json')
wiki_scrape = pd.read_json('data/scraped_page_data.json')

# Clean and prepare 'wiki_scrape' data
wiki_scrape['episode_number'] = wiki_scrape.episodeNumber
wiki_scrape['categories'] = wiki_scrape.wiki_categories.apply(
    lambda x: [item for item in x if item != "Episodes"] if isinstance(x, list) else []
)
wiki_scrape['people'] = wiki_scrape.appearance

# Function to rename columns except 'episode_number'
def prefix_columns(df, prefix):
    return df.rename(columns={col: f"{prefix}_{col}" for col in df.columns if col != 'episode_number'})

# Apply prefixing
raw_overlay = prefix_columns(raw_overlay, "overlay")
raw_final = prefix_columns(raw_final, "final")
nlp_guests = prefix_columns(nlp_guests, "nlp")
wiki_scrape = prefix_columns(wiki_scrape, "wiki")

# Merge all DataFrames on 'episode_number'
merged = raw_overlay.merge(raw_final, on='episode_number', how='outer') \
                    .merge(nlp_guests, on='episode_number', how='outer') \
                    .merge(wiki_scrape, on='episode_number', how='left')

# Sort by episode_number using natural sorting
merged = merged.sort_values(
    by="episode_number",
    key=natsort_keygen()
)

# Update categories that are edited on the wiki.
category_mask = (merged["wiki_episodeType"] != merged["final_categories"]) & (merged["final_categories"].apply(lambda x: len(x) if isinstance(x, list) else 0) == 0)
print("Episodes to copy category from wiki scrape", merged[category_mask].episode_number.to_list())
merged.loc[category_mask, "overlay_categories"] = merged.loc[category_mask, "wiki_episodeType"]

# Identify episodes without categories
missing_category_mask = (
    (merged["wiki_categories"].apply(lambda x: len(x) if isinstance(x, list) else 0) == 0) &
    (merged["final_categories"].apply(lambda x: len(x) if isinstance(x, list) else 0) == 0) &
    (merged["overlay_categories"].apply(lambda x: len(x) if isinstance(x, list) else 0) == 0)
)
print("Episodes with missing category", merged[missing_category_mask].episode_number.to_list())
# TODO: Add handling for episodes missing categories


# Convert date columns to datetime (errors='coerce' turns invalid values into NaT)
merged['final_release_date'] = pd.to_datetime(merged['final_release_date'], errors='coerce')
merged['final_coverage_end_date'] = pd.to_datetime(merged['final_coverage_end_date'], errors='coerce')

# Create a mask:
# Episode is considered "Present Day" if its coverage_end_date
# is within 3 weeks before the release_date.
present_day_mask = (
    (merged['final_coverage_end_date'] >= merged['final_release_date'] - pd.Timedelta(weeks=2)) &
    (merged['final_coverage_end_date'] <= merged['final_release_date'])
)

# For debugging or further processing, you can now work with the mask.
print("Episodes meeting the present_day condition:", merged[missing_category_mask & present_day_mask]['episode_number'].tolist())

merged.loc[missing_category_mask & present_day_mask, "overlay_categories"] = merged.loc[missing_category_mask & present_day_mask, "overlay_categories"].apply(lambda x: x if isinstance(x, list) else []).apply(lambda x: x + ["Present Day"])

# Update overlay_people with wiki_people if different
mask = merged["wiki_people"] != merged["final_people"]
merged.loc[mask, "overlay_people"] = merged.loc[mask, "wiki_people"]

# Use NLP people if no other people are available
no_non_nlp_people_mask = (
    (merged["wiki_people"].apply(lambda x: len(x) if isinstance(x, list) else 0) == 0) &
    (merged["final_people"].apply(lambda x: len(x) if isinstance(x, list) else 0) == 0) &
    (merged["overlay_people"].apply(lambda x: len(x) if isinstance(x, list) else 0) == 0)
)
merged.loc[no_non_nlp_people_mask, "overlay_people"] = merged.loc[no_non_nlp_people_mask, "nlp_people"]

# Count occurrences of each NLP person across all episodes, excluding NaN/null values
nlp_people_counts = Counter(
    person for people_set in merged["nlp_people"]
    if isinstance(people_set, list)  # Ensure it's iterable
    for person in people_set if person is not None and not pd.isna(person)
)

# Convert counts to fractions of total episodes
total_episodes = len(merged)
nlp_people_fractions = {person: count / total_episodes for person, count in nlp_people_counts.items()}

quit_flag = False  # Flag to exit the entire review process

# Iterate over merged in reverse order without re-sorting
for index, row in reversed(list(merged.iterrows())):
    if quit_flag:
        break  # Exit the outer loop if user chose to quit

    episode = row["episode_number"]
    nlp_people = row["nlp_people"] if isinstance(row["overlay_people"], list) else []
    overlay_people = row["overlay_people"] if isinstance(row["overlay_people"], list) else []
    final_people = row["final_people"] if isinstance(row["final_people"], list) else []
    wiki_people = row["wiki_people"] if isinstance(row["wiki_people"], list) else []

    authoritative_people = wiki_people + [p for p in final_people if p not in wiki_people]

    # Find missing people who are not in authoritative sources
    missing_people = set(nlp_people) - set(overlay_people) - set(authoritative_people)
    dirty = False  # Track if we've added someone

    if missing_people:
        # Sort by highest fraction
        sorted_missing = sorted(missing_people, key=lambda p: -nlp_people_fractions[p])
        preview_list = ", ".join(f"{p} ({nlp_people_fractions[p]*100:.1f}%)" for p in sorted_missing)

        print(f"\nEpisode {episode}")
        print(f"Current [overlay] guests: {', '.join(overlay_people) if overlay_people else 'None'}")
        print(f"Current [final] guests: {', '.join(final_people) if final_people else 'None'}")
        print(f"Current [wiki] guests: {', '.join(wiki_people) if wiki_people else 'None'}")
        print(f"Suggested additions: {preview_list}")

        # Get user input for each missing person
        for person in sorted_missing:
            # Convert to percentage
            fraction = nlp_people_fractions[person] * 100
            if fraction > NLP_THRESHOLD_FRACTION:
                continue
            response = input(f"Add {person} ({fraction:.1f}% of episodes) to authoritative list? (y/n/quit): ").strip().lower()

            if response == "quit":
                print("Exiting NLP review.")
                quit_flag = True  # Set flag to break out of outer loop
                break  # Exit inner loop immediately

            if response == "y":
                # Add to authoritative_people list
                authoritative_people.append(person)
                dirty = True  # Mark that we've added someone

        # Only update overlay if changes were made
        if dirty:
            print("WRITING", authoritative_people)
            merged.at[index, "overlay_people"] = authoritative_people

# Restore original overlay column names
overlay_columns = {col: col.replace("overlay_", "") for col in merged.columns if col.startswith("overlay_")}
overlay_columns["episode_number"] = "episode_number"

# Extract and rename overlay data
overlay_restored = merged[list(overlay_columns.keys())].rename(columns=overlay_columns)

# Filter out rows where all overlay fields (except episode_number) are empty
overlay_fields = [col for col in overlay_restored.columns if col != "episode_number"]
overlay_restored = overlay_restored.dropna(subset=overlay_fields, how='all')

# Sort by episode_number using natural sorting
overlay_restored = overlay_restored.sort_values(
    by="episode_number",
    key=natsort_keygen()
)

# Clean, drop null fields, and reorder keys in one step
cleaned_records = [
    OrderedDict([("episode_number", row["episode_number"])] +
                [(k, v) for k, v in row.dropna().drop("episode_number").items()])
    for _, row in overlay_restored.iterrows()
]

# Save to JSON
with open('data/overlay.json', 'w') as f:
    json.dump(cleaned_records, f, indent=2)

print("\nOverlay updated!")
