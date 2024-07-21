PREMISES 

a) Sensor Data Source:

 - The signals are assumed to come from two sensors: a demand sensor and a state sensor.
 - The API has two endpoints for receiving different data from these sensors.
 - The demanded signal is from the user requesting the elevator outside, not inside of it.

b) Timestamp Format:

 - It is assumed that the timestamps from both sensors are in the same format.
 - For this project, ISO 8601 format will be used.
 - If the timestamps would be in different formats, a preprocessing step will be necessary to convert them to the same format.

c) Time Frequency:

 - Given that the time frequencies of the sensor data could differ, every 10 seconds will be considered as the standard frequency for this project.

d) Standardizing Frequencies:

 - To standardize the data to the defined 10 seconds frequency, a fill forward method will be used.
 - This method will fill in any missing data points that were not transmitted by the sensors to ensure consistent data intervals.