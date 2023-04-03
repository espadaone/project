#  Project: "Visualization Medic drugs":

   In this project you can see how is etl in the python file and after that the pbix to see a little visualization 
   with different libraries like pandas, datetime and functions like transform_data, fill_empty_values and upload data can works with the ETL.
   After the etl when the information is upload are connected with azure mysql server to work with that data in power bi. 
   This work resolve a real situation job life when the medics are with all his little time to see how is going the information.
   The platform in a azure vm can get the python file and automatization with task scheduler to do the update or backup every day at the 5am.
   
   First you can see the python file and datasets where is working in this case with prescriptions . Prescriptions have the name of the different drugs and the "hmid" 
   that is the identification of each patient . This two columns serve to have a report in power bi when you can show the quantity of patients that is using a determine
   drug. 
   In the next pbix file you can see the report about the top 10 drugs that is more used and the quantity of patients having or using determine drug.
   
   
   
