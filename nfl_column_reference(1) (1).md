# NFL Play-by-Play Data Column Reference Guide

This dataset contains 372 columns of NFL play-by-play data for the 2024 season. Below is a comprehensive guide to each column and what it represents.

## Basic Game Information

| Column Name | Description |
|-------------|-------------|
| play_id | Unique identifier for each play within a game |
| game_id | Unique identifier for each game (format: YYYY_WW_AWAY_HOME) |
| old_game_id | Legacy game identifier from older data formats |
| home_team | Three-letter abbreviation for the home team |
| away_team | Three-letter abbreviation for the away team |
| season_type | Type of game (REG for regular season, POST for playoffs) |
| week | Week number of the season |
| game_date | Date the game was played |
| season | The NFL season year |

## Team and Possession Information

| Column Name | Description |
|-------------|-------------|
| posteam | Team with possession of the ball (offense) |
| posteam_type | Whether the possession team is "home" or "away" |
| defteam | Team on defense |
| side_of_field | Which team's side of the field the ball is on |
| yardline_100 | Yards to the goal line (0 = goal line, 100 = opposite goal line) |

## Game Clock and Timing

| Column Name | Description |
|-------------|-------------|
| quarter_seconds_remaining | Seconds remaining in the current quarter |
| half_seconds_remaining | Seconds remaining in the current half |
| game_seconds_remaining | Seconds remaining in the entire game |
| game_half | Which half of the game (Half1, Half2, Overtime) |
| quarter_end | Binary indicator if play ended the quarter |
| qtr | Quarter number (1-4, 5 for overtime) |
| time | Game clock time remaining in quarter (MM:SS format) |
| play_clock | Play clock time when play started |
| start_time | Actual start time of the play |
| time_of_day | Time of day when play occurred |
| end_clock_time | Game clock time when play ended |

## Drive Information

| Column Name | Description |
|-------------|-------------|
| drive | Drive number in the game |
| series | Unique series identifier within a drive |
| series_success | Whether the series resulted in a first down or touchdown |
| series_result | Outcome of the series (First down, Touchdown, etc.) |
| fixed_drive | Corrected drive number (fixes some data issues) |
| fixed_drive_result | Result of the corrected drive |
| drive_real_start_time | Actual start time of the drive |
| drive_play_count | Number of plays in the drive |
| drive_time_of_possession | Time of possession for the drive |
| drive_first_downs | Number of first downs in the drive |
| drive_inside20 | Whether drive reached inside the 20-yard line |
| drive_ended_with_score | Whether the drive ended with a score |
| drive_quarter_start | Quarter when drive started |
| drive_quarter_end | Quarter when drive ended |
| drive_yards_penalized | Total penalty yards in the drive |
| drive_start_transition | How the drive started (Kickoff, Punt, etc.) |
| drive_end_transition | How the drive ended (Touchdown, Field Goal, etc.) |
| drive_game_clock_start | Game clock when drive started |
| drive_game_clock_end | Game clock when drive ended |
| drive_start_yard_line | Yard line where drive started |
| drive_end_yard_line | Yard line where drive ended |
| drive_play_id_started | Play ID where drive started |
| drive_play_id_ended | Play ID where drive ended |

## Down and Distance

| Column Name | Description |
|-------------|-------------|
| down | Down number (1-4) |
| ydstogo | Yards to go for a first down |
| ydsnet | Net yards gained on the drive so far |
| goal_to_go | Binary indicator if it's a goal-to-go situation |
| yrdln | Yard line where play occurred (team abbreviation + number) |

## Play Description and Type

| Column Name | Description |
|-------------|-------------|
| desc | Text description of the play |
| play_type | Simplified play type (pass, run, punt, etc.) |
| play_type_nfl | Official NFL play type designation |
| special_teams_play | Binary indicator for special teams plays |
| st_play_type | Type of special teams play |
| sp | Binary indicator for special teams play |
| aborted_play | Binary indicator for aborted plays |
| play_deleted | Binary indicator for deleted plays |

## Play Outcome

| Column Name | Description |
|-------------|-------------|
| yards_gained | Total yards gained on the play |
| success | Binary indicator for successful play (EPA > 0) |
| first_down | Binary indicator if play resulted in first down |
| touchdown | Binary indicator if play resulted in touchdown |
| safety | Binary indicator if play resulted in safety |

## Passing Plays

| Column Name | Description |
|-------------|-------------|
| pass_attempt | Binary indicator for pass attempt |
| complete_pass | Binary indicator for completed pass |
| incomplete_pass | Binary indicator for incomplete pass |
| interception | Binary indicator for interception |
| pass_touchdown | Binary indicator for passing touchdown |
| pass_length | Length of pass (short, deep) |
| pass_location | Location of pass (left, middle, right) |
| air_yards | Yards the ball traveled in the air |
| yards_after_catch | Yards gained after the catch |
| qb_dropback | Binary indicator for QB dropback |
| qb_kneel | Binary indicator for QB kneel |
| qb_spike | Binary indicator for QB spike |
| qb_scramble | Binary indicator for QB scramble |
| qb_hit | Binary indicator if QB was hit |
| sack | Binary indicator for sack |
| shotgun | Binary indicator for shotgun formation |
| no_huddle | Binary indicator for no-huddle offense |
| pass | Binary indicator for pass play |
| xpass | Expected pass probability based on situation |
| pass_oe | Pass over expectation (actual - expected pass rate) |

## Rushing Plays

| Column Name | Description |
|-------------|-------------|
| rush_attempt | Binary indicator for rush attempt |
| rush_touchdown | Binary indicator for rushing touchdown |
| run_location | Location of run (left, middle, right) |
| run_gap | Gap where run occurred (guard, tackle, end) |
| rush | Binary indicator for rush play |

## Kicking Plays

| Column Name | Description |
|-------------|-------------|
| field_goal_attempt | Binary indicator for field goal attempt |
| field_goal_result | Result of field goal (made, missed, blocked) |
| kick_distance | Distance of the kick attempt |
| extra_point_attempt | Binary indicator for extra point attempt |
| extra_point_result | Result of extra point attempt |
| two_point_attempt | Binary indicator for two-point conversion attempt |
| two_point_conv_result | Result of two-point conversion |
| kickoff_attempt | Binary indicator for kickoff |
| punt_attempt | Binary indicator for punt |

## Special Teams Events

| Column Name | Description |
|-------------|-------------|
| punt_blocked | Binary indicator for blocked punt |
| touchback | Binary indicator for touchback |
| punt_inside_twenty | Binary indicator for punt inside 20 |
| punt_in_endzone | Binary indicator for punt in endzone |
| punt_out_of_bounds | Binary indicator for punt out of bounds |
| punt_downed | Binary indicator for downed punt |
| punt_fair_catch | Binary indicator for fair catch on punt |
| kickoff_inside_twenty | Binary indicator for kickoff inside 20 |
| kickoff_in_endzone | Binary indicator for kickoff in endzone |
| kickoff_out_of_bounds | Binary indicator for kickoff out of bounds |
| kickoff_downed | Binary indicator for downed kickoff |
| kickoff_fair_catch | Binary indicator for fair catch on kickoff |
| return_touchdown | Binary indicator for return touchdown |
| own_kickoff_recovery | Binary indicator for onside kick recovery |
| own_kickoff_recovery_td | Binary indicator for onside kick recovery TD |

## Scoring and Game State

| Column Name | Description |
|-------------|-------------|
| total_home_score | Total home team score |
| total_away_score | Total away team score |
| posteam_score | Score of the team with possession |
| defteam_score | Score of the defensive team |
| score_differential | Score difference (posteam - defteam) |
| posteam_score_post | Possession team score after the play |
| defteam_score_post | Defensive team score after the play |
| score_differential_post | Score difference after the play |
| away_score | Away team score |
| home_score | Home team score |
| td_team | Team that scored the touchdown |
| td_player_name | Name of player who scored touchdown |
| td_player_id | ID of player who scored touchdown |

## Timeouts

| Column Name | Description |
|-------------|-------------|
| home_timeouts_remaining | Timeouts remaining for home team |
| away_timeouts_remaining | Timeouts remaining for away team |
| posteam_timeouts_remaining | Timeouts remaining for possession team |
| defteam_timeouts_remaining | Timeouts remaining for defensive team |
| timeout | Binary indicator for timeout |
| timeout_team | Team that called the timeout |

## Expected Points (EPA) Analysis

| Column Name | Description |
|-------------|-------------|
| ep | Expected points before the play |
| epa | Expected points added by the play |
| total_home_epa | Cumulative EPA for home team |
| total_away_epa | Cumulative EPA for away team |
| total_home_rush_epa | Cumulative rushing EPA for home team |
| total_away_rush_epa | Cumulative rushing EPA for away team |
| total_home_pass_epa | Cumulative passing EPA for home team |
| total_away_pass_epa | Cumulative passing EPA for away team |
| air_epa | EPA from air yards on pass plays |
| yac_epa | EPA from yards after catch |
| comp_air_epa | Air EPA on completed passes only |
| comp_yac_epa | YAC EPA on completed passes only |
| total_home_comp_air_epa | Cumulative completed air EPA for home team |
| total_away_comp_air_epa | Cumulative completed air EPA for away team |
| total_home_comp_yac_epa | Cumulative completed YAC EPA for home team |
| total_away_comp_yac_epa | Cumulative completed YAC EPA for away team |
| total_home_raw_air_epa | Cumulative raw air EPA for home team |
| total_away_raw_air_epa | Cumulative raw air EPA for away team |
| total_home_raw_yac_epa | Cumulative raw YAC EPA for home team |
| total_away_raw_yac_epa | Cumulative raw YAC EPA for away team |
| qb_epa | Expected points added attributed to quarterback |

## Win Probability (WP) Analysis

| Column Name | Description |
|-------------|-------------|
| wp | Win probability for possession team before play |
| def_wp | Win probability for defensive team before play |
| home_wp | Win probability for home team before play |
| away_wp | Win probability for away team before play |
| wpa | Win probability added by the play |
| home_wp_post | Home team win probability after play |
| away_wp_post | Away team win probability after play |
| vegas_wp | Vegas-based win probability |
| vegas_home_wp | Vegas-based home team win probability |
| vegas_wpa | Vegas-based win probability added |
| vegas_home_wpa | Vegas-based home team WPA |
| total_home_rush_wpa | Cumulative rushing WPA for home team |
| total_away_rush_wpa | Cumulative rushing WPA for away team |
| total_home_pass_wpa | Cumulative passing WPA for home team |
| total_away_pass_wpa | Cumulative passing WPA for away team |
| air_wpa | WPA from air yards |
| yac_wpa | WPA from yards after catch |
| comp_air_wpa | Air WPA on completed passes |
| comp_yac_wpa | YAC WPA on completed passes |
| total_home_comp_air_wpa | Cumulative completed air WPA for home team |
| total_away_comp_air_wpa | Cumulative completed air WPA for away team |
| total_home_comp_yac_wpa | Cumulative completed YAC WPA for home team |
| total_away_comp_yac_wpa | Cumulative completed YAC WPA for away team |
| total_home_raw_air_wpa | Cumulative raw air WPA for home team |
| total_away_raw_air_wpa | Cumulative raw air WPA for away team |
| total_home_raw_yac_wpa | Cumulative raw YAC WPA for home team |
| total_away_raw_yac_wpa | Cumulative raw YAC WPA for away team |

## Situational Probabilities

| Column Name | Description |
|-------------|-------------|
| no_score_prob | Probability of no score on drive |
| opp_fg_prob | Probability opponent scores field goal |
| opp_safety_prob | Probability opponent scores safety |
| opp_td_prob | Probability opponent scores touchdown |
| fg_prob | Probability of field goal |
| safety_prob | Probability of safety |
| td_prob | Probability of touchdown |
| extra_point_prob | Probability of extra point success |
| two_point_conversion_prob | Probability of two-point conversion success |

## Down Conversion Tracking

| Column Name | Description |
|-------------|-------------|
| first_down_rush | Binary indicator for first down via rush |
| first_down_pass | Binary indicator for first down via pass |
| first_down_penalty | Binary indicator for first down via penalty |
| third_down_converted | Binary indicator for third down conversion |
| third_down_failed | Binary indicator for third down failure |
| fourth_down_converted | Binary indicator for fourth down conversion |
| fourth_down_failed | Binary indicator for fourth down failure |

## Fumbles and Turnovers

| Column Name | Description |
|-------------|-------------|
| fumble | Binary indicator for fumble |
| fumble_forced | Binary indicator for forced fumble |
| fumble_not_forced | Binary indicator for unforced fumble |
| fumble_out_of_bounds | Binary indicator for fumble out of bounds |
| fumble_lost | Binary indicator for lost fumble |

## Tackles and Defensive Stats

| Column Name | Description |
|-------------|-------------|
| solo_tackle | Binary indicator for solo tackle |
| assist_tackle | Binary indicator for assisted tackle |
| tackle_with_assist | Binary indicator for tackle with assist |
| tackled_for_loss | Binary indicator for tackle for loss |

## Penalties

| Column Name | Description |
|-------------|-------------|
| penalty | Binary indicator for penalty |
| penalty_team | Team that committed the penalty |
| penalty_player_id | ID of player who committed penalty |
| penalty_player_name | Name of player who committed penalty |
| penalty_yards | Yards assessed for penalty |
| penalty_type | Type of penalty committed |

## Replay and Challenges

| Column Name | Description |
|-------------|-------------|
| replay_or_challenge | Binary indicator for replay/challenge |
| replay_or_challenge_result | Result of replay/challenge |

## Player Information - Passing

| Column Name | Description |
|-------------|-------------|
| passer_player_id | Unique ID of the quarterback/passer |
| passer_player_name | Name of the quarterback/passer |
| passer | Shortened name of passer |
| passer_jersey_number | Jersey number of passer |
| passer_id | Alternative passer ID |
| passing_yards | Yards gained passing on this play |

## Player Information - Receiving

| Column Name | Description |
|-------------|-------------|
| receiver_player_id | Unique ID of the receiver |
| receiver_player_name | Name of the receiver |
| receiver | Shortened name of receiver |
| receiver_jersey_number | Jersey number of receiver |
| receiver_id | Alternative receiver ID |
| receiving_yards | Yards gained receiving on this play |

## Player Information - Rushing

| Column Name | Description |
|-------------|-------------|
| rusher_player_id | Unique ID of the rusher |
| rusher_player_name | Name of the rusher |
| rusher | Shortened name of rusher |
| rusher_jersey_number | Jersey number of rusher |
| rusher_id | Alternative rusher ID |
| rushing_yards | Yards gained rushing on this play |

## Lateral Play Information

| Column Name | Description |
|-------------|-------------|
| lateral_reception | Binary indicator for lateral reception |
| lateral_rush | Binary indicator for lateral rush |
| lateral_return | Binary indicator for lateral return |
| lateral_recovery | Binary indicator for lateral recovery |
| lateral_receiver_player_id | ID of lateral receiver |
| lateral_receiver_player_name | Name of lateral receiver |
| lateral_receiving_yards | Yards on lateral reception |
| lateral_rusher_player_id | ID of lateral rusher |
| lateral_rusher_player_name | Name of lateral rusher |
| lateral_rushing_yards | Yards on lateral rush |
| lateral_sack_player_id | ID of player with lateral sack |
| lateral_sack_player_name | Name of player with lateral sack |
| lateral_interception_player_id | ID of lateral interception player |
| lateral_interception_player_name | Name of lateral interception player |
| lateral_punt_returner_player_id | ID of lateral punt returner |
| lateral_punt_returner_player_name | Name of lateral punt returner |
| lateral_kickoff_returner_player_id | ID of lateral kickoff returner |
| lateral_kickoff_returner_player_name | Name of lateral kickoff returner |

## Return Specialists

| Column Name | Description |
|-------------|-------------|
| punt_returner_player_id | ID of punt returner |
| punt_returner_player_name | Name of punt returner |
| kickoff_returner_player_name | Name of kickoff returner |
| kickoff_returner_player_id | ID of kickoff returner |
| return_team | Team making the return |
| return_yards | Yards gained on return |

## Kickers and Punters

| Column Name | Description |
|-------------|-------------|
| punter_player_id | ID of punter |
| punter_player_name | Name of punter |
| kicker_player_name | Name of kicker |
| kicker_player_id | ID of kicker |

## Defensive Players - Interceptions

| Column Name | Description |
|-------------|-------------|
| interception_player_id | ID of player with interception |
| interception_player_name | Name of player with interception |

## Defensive Players - Blocks

| Column Name | Description |
|-------------|-------------|
| blocked_player_id | ID of player who blocked kick |
| blocked_player_name | Name of player who blocked kick |
| own_kickoff_recovery_player_id | ID of player recovering own kickoff |
| own_kickoff_recovery_player_name | Name of player recovering own kickoff |

## Defensive Players - Tackles for Loss

| Column Name | Description |
|-------------|-------------|
| tackle_for_loss_1_player_id | ID of first player with tackle for loss |
| tackle_for_loss_1_player_name | Name of first player with tackle for loss |
| tackle_for_loss_2_player_id | ID of second player with tackle for loss |
| tackle_for_loss_2_player_name | Name of second player with tackle for loss |

## Defensive Players - QB Hits

| Column Name | Description |
|-------------|-------------|
| qb_hit_1_player_id | ID of first player hitting QB |
| qb_hit_1_player_name | Name of first player hitting QB |
| qb_hit_2_player_id | ID of second player hitting QB |
| qb_hit_2_player_name | Name of second player hitting QB |

## Defensive Players - Forced Fumbles

| Column Name | Description |
|-------------|-------------|
| forced_fumble_player_1_team | Team of first player forcing fumble |
| forced_fumble_player_1_player_id | ID of first player forcing fumble |
| forced_fumble_player_1_player_name | Name of first player forcing fumble |
| forced_fumble_player_2_team | Team of second player forcing fumble |
| forced_fumble_player_2_player_id | ID of second player forcing fumble |
| forced_fumble_player_2_player_name | Name of second player forcing fumble |

## Defensive Players - Solo Tackles

| Column Name | Description |
|-------------|-------------|
| solo_tackle_1_team | Team of first solo tackler |
| solo_tackle_2_team | Team of second solo tackler |
| solo_tackle_1_player_id | ID of first solo tackler |
| solo_tackle_2_player_id | ID of second solo tackler |
| solo_tackle_1_player_name | Name of first solo tackler |
| solo_tackle_2_player_name | Name of second solo tackler |

## Defensive Players - Assist Tackles

| Column Name | Description |
|-------------|-------------|
| assist_tackle_1_player_id | ID of first assist tackler |
| assist_tackle_1_player_name | Name of first assist tackler |
| assist_tackle_1_team | Team of first assist tackler |
| assist_tackle_2_player_id | ID of second assist tackler |
| assist_tackle_2_player_name | Name of second assist tackler |
| assist_tackle_2_team | Team of second assist tackler |
| assist_tackle_3_player_id | ID of third assist tackler |
| assist_tackle_3_player_name | Name of third assist tackler |
| assist_tackle_3_team | Team of third assist tackler |
| assist_tackle_4_player_id | ID of fourth assist tackler |
| assist_tackle_4_player_name | Name of fourth assist tackler |
| assist_tackle_4_team | Team of fourth assist tackler |

## Defensive Players - Tackles with Assist

| Column Name | Description |
|-------------|-------------|
| tackle_with_assist_1_player_id | ID of first player in tackle with assist |
| tackle_with_assist_1_player_name | Name of first player in tackle with assist |
| tackle_with_assist_1_team | Team of first player in tackle with assist |
| tackle_with_assist_2_player_id | ID of second player in tackle with assist |
| tackle_with_assist_2_player_name | Name of second player in tackle with assist |
| tackle_with_assist_2_team | Team of second player in tackle with assist |

## Defensive Players - Pass Defense

| Column Name | Description |
|-------------|-------------|
| pass_defense_1_player_id | ID of first player with pass defense |
| pass_defense_1_player_name | Name of first player with pass defense |
| pass_defense_2_player_id | ID of second player with pass defense |
| pass_defense_2_player_name | Name of second player with pass defense |

## Fumble Details

| Column Name | Description |
|-------------|-------------|
| fumbled_1_team | Team of first player who fumbled |
| fumbled_1_player_id | ID of first player who fumbled |
| fumbled_1_player_name | Name of first player who fumbled |
| fumbled_2_player_id | ID of second player who fumbled |
| fumbled_2_player_name | Name of second player who fumbled |
| fumbled_2_team | Team of second player who fumbled |

## Fumble Recovery Details

| Column Name | Description |
|-------------|-------------|
| fumble_recovery_1_team | Team recovering first fumble |
| fumble_recovery_1_yards | Yards gained on first fumble recovery |
| fumble_recovery_1_player_id | ID of first fumble recovery player |
| fumble_recovery_1_player_name | Name of first fumble recovery player |
| fumble_recovery_2_team | Team recovering second fumble |
| fumble_recovery_2_yards | Yards gained on second fumble recovery |
| fumble_recovery_2_player_id | ID of second fumble recovery player |
| fumble_recovery_2_player_name | Name of second fumble recovery player |

## Sack Information

| Column Name | Description |
|-------------|-------------|
| sack_player_id | ID of player with full sack |
| sack_player_name | Name of player with full sack |
| half_sack_1_player_id | ID of first player with half sack |
| half_sack_1_player_name | Name of first player with half sack |
| half_sack_2_player_id | ID of second player with half sack |
| half_sack_2_player_name | Name of second player with half sack |

## Safety Information

| Column Name | Description |
|-------------|-------------|
| safety_player_name | Name of player credited with safety |
| safety_player_id | ID of player credited with safety |

## Defensive Conversion Attempts

| Column Name | Description |
|-------------|-------------|
| defensive_two_point_attempt | Binary indicator for defensive 2-point attempt |
| defensive_two_point_conv | Binary indicator for successful defensive 2-point |
| defensive_extra_point_attempt | Binary indicator for defensive extra point attempt |
| defensive_extra_point_conv | Binary indicator for successful defensive extra point |

## Advanced Metrics

| Column Name | Description |
|-------------|-------------|
| cp | Completion probability of the pass |
| cpoe | Completion percentage over expected |
| xyac_epa | Expected yards after catch EPA |
| xyac_mean_yardage | Expected yards after catch (mean) |
| xyac_median_yardage | Expected yards after catch (median) |
| xyac_success | Binary indicator for successful XYAC |
| xyac_fd | Binary indicator for XYAC first down |

## Game Metadata

| Column Name | Description |
|-------------|-------------|
| order_sequence | Sequence order of play in game |
| nfl_api_id | Official NFL API identifier |
| stadium | Name of stadium where game was played |
| stadium_id | Unique identifier for stadium |
| game_stadium | Stadium information for the game |
| weather | Weather conditions during game |
| location | Game location (home/away designation) |
| result | Final result of the game |
| total | Total points scored in game |
| spread_line | Point spread for the game |
| total_line | Over/under line for the game |
| div_game | Binary indicator for divisional game |
| roof | Type of roof (dome, outdoors, etc.) |
| surface | Playing surface type (grass, turf, etc.) |
| temp | Temperature during game |
| wind | Wind conditions during game |
| home_coach | Name of home team head coach |
| away_coach | Name of away team head coach |
| home_opening_kickoff | Binary indicator if home team kicked off first |

## Fantasy Football

| Column Name | Description |
|-------------|-------------|
| fantasy_player_name | Player name for fantasy purposes |
| fantasy_player_id | Player ID for fantasy purposes |
| fantasy | Binary indicator for fantasy relevant play |
| fantasy_id | Fantasy-specific player identifier |

## Additional Play Context

| Column Name | Description |
|-------------|-------------|
| out_of_bounds | Binary indicator if play went out of bounds |
| special | Binary indicator for special teams play |
| play | Binary indicator for regular play |
| end_yard_line | Yard line where play ended |

## Generic Player Information

| Column Name | Description |
|-------------|-------------|
| name | Generic player name field |
| jersey_number | Generic jersey number field |
| id | Generic player ID field |

---

## Notes for Using This Data

1. **EPA (Expected Points Added)**: This is one of the most important advanced metrics. It measures how much a play increases or decreases a team's expected points.

2. **Win Probability**: Shows how much each play affects a team's chances of winning.

3. **Player IDs**: Most player information is duplicated in multiple formats for compatibility with different systems.

4. **Binary Indicators**: Many columns are 1/0 indicators. 1 means the event happened, 0 means it didn't.

5. **Team Abbreviations**: Use standard NFL 3-letter abbreviations (e.g., NE, GB, DAL).

6. **Missing Data**: Some advanced metrics may be NA for certain play types where they don't apply.

This dataset is incredibly rich and perfect for NFL analytics. You can analyze everything from basic team performance to advanced situational metrics and player efficiency!
