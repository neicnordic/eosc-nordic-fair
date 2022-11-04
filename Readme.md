#### FAIR score analyzer script

This tool is meant for analyzing datasets based on FAIR principle. It uses google sheet with datasets on each row and each dataset is pushed to evaluator. Outcome of this is FAIR score of the dataset. It counts individual F, A, I R and overall score.

##### Guide:
1. Install requirements
2. Copy Google sheet url to config.ini
3. Copy evaluator API url to config.ini
4. After initial run, you need to authorize google access using web browser. This will create ceredntials files.
5. In Google sheet cell O2, there should be a checkbox for launching or stopping the script.

Google sheet template: Columns A to O are reserved for analyze results:

A - repository ID;  
B - dataset ID;  
E - F component of FAIR;  
F - A component of FAIR;  
G - I component of FAIR;  
H - R component of FAIR;  
I - FAIR in total;  
J - number of passed tests/number of total tests;  
K - status of the analyze (Analyzing/Ready);  
L - analyze start time;  
M - analyze end time;  
N - calculation time;  
O - Run script checkbox;  