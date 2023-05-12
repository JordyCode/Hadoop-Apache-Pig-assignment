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

/* Use the FOREACH operator to change the "head_shots" column from a chararray to an integer.
If you do this in the STREAM operator, it will not see the values of the "head_shots". */
file_data_records = FOREACH file_data_records GENERATE 
    date,
    time_of_day,
    placed,
    mental_state,
    eliminations,
    assists,
    revives,
    accuracy,
    hits,
    (int)head_shots AS head_shots:int,
    distance_traveled,
    materials_gathered,
    materials_used,
    damage_taken,
    damage_to_players,
    damage_to_structures;

/* Use the GROUP operator to group the data by "mental_state". */
head_shots_by_mental_state = GROUP file_data_records BY mental_state;

/* Use the FOREACH operator to calculate the sum of "head_shots" for each group and 
the result is stored in a new column called "total_head_shots". */
result = FOREACH head_shots_by_mental_state GENERATE group, 
SUM(file_data_records.head_shots) AS total_head_shots;

/* Display the result of the total headshots grouped by the mental state */
DUMP result;