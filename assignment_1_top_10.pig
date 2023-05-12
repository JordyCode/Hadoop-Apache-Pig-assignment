/* Load the CSV file into Pig using the PigStorage function and specify the delimiter as a comma. */
fortnite_csv = LOAD '/user/maria_dev/FortniteStatistics.csv' USING PigStorage(','); 

/* Use the STREAM operator to read the data from the CSV file and skip the header row. */
file_data_records = STREAM fortnite_csv THROUGH `tail -n +2` AS
	(date:chararray,
    time_of_day:chararray,
    placed:chararray,
    mental_state:chararray,
    eliminations:chararray,
    assists:chararray,
    revives:chararray,
    accuracy:chararray,
    hits:chararray,
    head_shots:chararray,
    distance_traveled:chararray,
    materials_gathered:chararray,
    materials_used:chararray,
    damage_taken:chararray,
	damage_to_players:chararray,
    damage_to_structures:chararray);

/* Group the data by "date" and count the number of rows for each "date" */
grouped_data = GROUP file_data_records BY date;
date_counts = FOREACH grouped_data GENERATE group AS date, COUNT(file_data_records) AS count;

/* Sort the "date_counts" in descending order and take the top 10 */
top_dates = ORDER date_counts BY count DESC;
top_10_dates = LIMIT top_dates 10;

/* Output the top 10 most popular days/dates */
DUMP top_10_dates;
