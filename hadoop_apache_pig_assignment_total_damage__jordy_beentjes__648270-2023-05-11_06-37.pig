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
    
/* Use the FOREACH operator to go through all the variables and use the "REPLACE" operator 
to remove the percentage symbol (%) from the accuracy.
If you do this in the STREAM operator, it will not see the values of the "accuracy". */
file_data_records = FOREACH file_data_records GENERATE
    date,
    time_of_day,
    placed,
    mental_state,
    eliminations,
    assists,
    revives,
    REPLACE(accuracy, '%', '') AS accuracy:chararray,
    hits,
    head_shots,
    distance_traveled,
    materials_gathered,
    materials_used,
    damage_taken,
    damage_to_players,
    damage_to_structures;

/* Filter the records on "accuracy" between 90 and 100% */
filtered_data_records = FILTER file_data_records BY ((INT)accuracy >= 90) AND ((INT)accuracy <= 100);

/* Filter the "damage_to_players" based on the "filtered_data_records" on "accuracy" */
filtered_damage_to_players = FOREACH filtered_data_records GENERATE (INT)damage_to_players;

/* Calculate the "total_damage" to players in the "filtered_damage_to_players" record */
/* This line is commented because we only have one value which can't be counted and results in an error */
/* total_damage = SUM(filtered_damage_to_players); */

/* Display the total damage to players */
DUMP filtered_damage_to_players;