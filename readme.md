First of all apache airflow should be installed on the computer.
Then we modify the ~/airflow/airflow.cfg file, where we update the path where our dags are present also we set the value of false for default dags examples and default dags connection from the same cfg files.
Then we run the airflow on standalone mode by typing airflow standalone in terminal.
![data pipeline]("pipeline.png")
