if __name__ == '__main__':
    import pandas as pd
    from collections import Counter
    from pprint import pprint
    merged_df = pd.read_csv('merged.csv')

    categories = pd.DataFrame(Counter(merged_df.episode_type.to_list()).items(), columns=['Category', 'Number'])

    with open("categories.csv", "w", encoding='utf-8') as csv_file:
        csv_file.write(categories.to_csv(index=False, line_terminator='\n'))
