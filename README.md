## Fusemachines Final ETL Project
 

*Author:*   
Bipin Ghimire - bipin.ghimire@fusemachines.com  
Rojesh Pradhananga - rojesh.pradhananga@fusemachines.com  
Ujjwal Khadka - khadka.ujjwal@fusemachines.com  


## Dataset Summary 
Link : https://www.kaggle.com/datasets/lava18/google-play-store-apps 
- Each row in the dataset represents an Android app available on the Google Play Store.
- The dataset includes information about each app's category, user rating, size, review, installs, price, genres and more.
The raw dataset includes the following columns:  

1. **App**: The name or title of the mobile application.  

2. **Category**: The category or genre to which the app belongs (e.g., "ART_AND_DESIGN").
3. **Rating**: The user rating of the app (e.g., 4.1).
4. **Reviews**: The number of user reviews for the app (e.g., 159).
5. **Size**: The size of the app (e.g., "19M").
6. **Installs**: The number of times the app has been installed (e.g., "10,000+").
7. **Type**: The type of the app, which can be "Free" or "Paid."
8. **Price**: The price of the app, if it is a paid app (e.g., 0 for free apps).
9. **Content Rating**: The content rating for the app (e.g., "Everyone").
10. **Genres**: The genre(s) associated with the app (e.g., "Art & Design; Pretend Play").
11. **Last Updated**: The date when the app was last updated (e.g., "7-Jan-18").
12. **Current Ver**: The current version of the app (e.g., "1.0.0").
13. **Android Ver**: The minimum Android version required to run the app (e.g., "4.0.3 and up").



## Prerequisites

### a. Virtual Environment
- Create a virtual environment

      pip install virtualenv

      virtualenv <Your environment name>
- Install Apache Airflow inside the environment.

      pip install "apache-airflow[celery]==2.7.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.8.txt"  

   Note: In the contraints specify your python version.
 
- A folder named airflow will now be created in you /home/user directory

- create a new folder named dags inside airflow folder.

      mkdir dags

- Verify the installation running the below command in  your bash:  

      apache standalone

### b. Running above code  

- Activate the virtual environment.  

      source <Your environment name>/bin/activate

- Clone the repository

      git clone https://github.com/BIPIN-GHIMIRE-FUSEMACHINES/FInal-Project-  

- Create a .env file inside the repository.

      touch .env

- Move the python script inside the dag folder to the dags folder inside your airflow folder.

      mv <path_to_repository>/dag/final_project.py <path_to_airflow_folder>/dags  

- Run airflow 

      airflow standalone

- Run the dag manually from the airflow UI.