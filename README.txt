Implement: Spark
Programming Language: Python3

Environment: Windows10
Setup: 1. Download spark-2.1.1-bin-hadoop2.7.tgz from https://spark.apache.org/downloads.html
       2. Extract it into D drive
       3. Set environmental variables: SPARK_HOME, D:\spark-2.1.1-bin-hadoop2.7
                                       PATH,       D:\spark\spark-1.6.1-bin-hadoop2.6\bin
       4. Download Windows Utilities form https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1/bin
       5. Extract it into D:\spark\spark-1.6.1-bin-hadoop2.6\bin
       6. Check the Python environment variables

Source codes: problem01.py
              problem02.py
              problem03.py
              problem04.py

Compile: 1. Open the cmd and type cd /d PATH (the file exist)
         2. Type spark-submit FILE_NAME (ex: problem01.py)
         3. It will print the top 20 result (the number of the printed result can be changed by edit the source code)

Because that the Café in the venue_info.txt caused the decode error in Python3.
Python3 used 'cp950' (standard of Big5) to decode '\xe9' (é) and occurred error.
So we created the file new_venue_info.txt, changing all the Café into Cafe.
And used the new file to run problem 3.

Efficiency: problem01.py, 7.905024s
            problem02.py, 7.228653s
            problem03.py, 16.436811s
            problem04.py, 9.705130s

The uploaded files only include source codes and new_venue_info.txt, which is created by myself.
Please use new_venue_info.txt to run problem03.py. Thank you.
