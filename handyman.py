#!/usr/bin/env python
# coding: utf-8

# In[26]:


class handyman ():
    def json_csv (filepath):
        import json
        import csv
        from os import walk
        
        filenames = next(walk(filepath), (None, None, []))[2]  # [] if no file
        filenames = [file for file in filenames if file.endswith('.json')]
        
        for file in filenames:
            with open(f'{filepath}{file}') as json_file:
                jsondata  = json.load(json_file)

            data_file = open(f'{filepath}{file}.csv', 'w', newline='', encoding="utf-8")
            csv_writer = csv.writer(data_file)
            count = 0
            count = 0
            for data in jsondata:
                if count == 0:
                    header = data.keys()
                    csv_writer.writerow(header)
                    count += 1
                csv_writer.writerow(data.values())

        data_file.close()
        
        
    def df_parquet(df, filepath, filename):
        
        import os
        import parquet
        import dbutils
        
        # Create single partition of spark instance and write parquet as a folder (spark default)
        df.repartition(1).write.mode('overwrite').parquet(os.path.join(filepath, 'tmp_' + filename))

        # Select the parquet-file out of the 'spark'-folder
        all_files = dbutils.fs.ls(os.path.join(filepath, 'tmp_' + filename))
        all_parquet_files = []
        for file in all_files:
            if file.path[-8:]=='.parquet' and file.name[0] != '_':
                all_parquet_files.append(file.path)

        assert len(all_parquet_files)==1,f"There are more than one parquet-files (i.e. {len(all_parquet_files)}) created in the spark-folder. You should make sure you repartition the spark dataframe into one."

        # Copy the parquet-file out of the spark folder to the desired location
        dbutils.fs.cp(all_parquet_files[0], os.path.join(filepath, filename))

        # Remove the spark-folder
        dbutils.fs.rm(os.path.join(filepath, 'tmp_' + filename), True)
        
        
    def csv_parquet (filepath):
        import csv
        from os import walk

        filenames = next(walk(filepath), (None, None, []))[2]  # [] if no file

        files_csv = [file for file in filenames if file.endswith('.csv')]

        for file in files_csv:
          filename = file.replace('.csv', '.parquet')
          df = pd.read.options(header= True, multiLine= True, inferSchema='True', delimiter=';').csv(f'{filepath}{file}')
          handyman.df_parquet(df, filepath, filename)

