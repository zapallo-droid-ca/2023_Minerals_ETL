##-- Libraries and Packages
import os
import pandas as pd

def filesinpath_funct(path, extensions = ['csv']):
    
    if len(os.listdir(path)) != 1:      
        filesinpath = [x for x in os.listdir(path) if not x.startswith('~')] #to avoid open files / temporal copies    
    else:
        filesinpath = os.listdir(path)
                
    filesinpath = pd.DataFrame(filesinpath, columns = ['file'])
    filesinpath['type'] = filesinpath.file.str.split('.').str[-1]
    filesinpath = filesinpath[filesinpath['type'].isin(extensions)].reset_index(drop = True)
    
    print(f'There are {filesinpath.shape[0]} files in the given path')    

    filesinpathSize = pd.DataFrame()
    
    if filesinpath.shape[0] != 1:    
        print('Loading file sizes')
        for file in filesinpath.file.unique():
            dataTemp = pd.DataFrame([{'file': file, 
                                      'size (MB)': os.path.getsize(path + '/' + file) / 1000000}])
            
            filesinpathSize = pd.concat([filesinpathSize,dataTemp])
            
        filesinpath = filesinpath.merge(filesinpathSize, how = 'left', on = 'file')
        
        filesinpath['fileWeight'] = (filesinpath['size (MB)'] / filesinpath['size (MB)'].sum()) * 100 
        
        print(f"""Process finished, the path contains {filesinpath.shape[0]} files with a total size of {filesinpath['size (MB)'].sum()} MB""")
        
    elif filesinpath.shape[0] == 1:  #case, one file
        print('Loading file sizes')
        file = filesinpath.file[0]
        dataTemp = pd.DataFrame([{'file': file, 
                                  'size (MB)': os.path.getsize(path + '/' + file) / 1000000}])
            
        filesinpathSize = pd.concat([filesinpathSize,dataTemp])
            
        filesinpath = filesinpath.merge(filesinpathSize, how = 'left', on = 'file')
        
        filesinpath['fileWeight'] = (filesinpath['size (MB)'] / filesinpath['size (MB)'].sum()) * 100 
        
        print(f"""Process finished, the path contains {filesinpath.shape[0]} files with a total size of {filesinpath['size (MB)'].sum()} MB""")            
    
    else:
        print('there are non compatibilities with the function parameters')
        
    return filesinpath
       
        
def df_pivot_wnames(data,index_,columns_,values_,fillnas = True):
    
    """
        function doc:
            the function is based on pd.pivot, but it is taking the multi-index order (0) and assigning the values in columns as suffixes:
            * data = pandas dataframe
            * index_ = same index parameter as pd.pivot
            * columns_ = same columns parameter as pd.pivot
            * values_ = same values parameter as pd.pivot
            * fillnas = if fillna(0) would be apoplied
            * columns_values = number of values in the variable passed as columns_, this is the number of times that the values_ labels will be written to be concatated with the values_ name
    """
    
    columns_values = len(data[columns_].iloc[:,0].unique())
    
    if fillnas:
        data_pivot = data.pivot(index = index_, columns = columns_, values = values_).reset_index(col_level = 0).fillna(0)
    else:
        data_pivot = data.pivot(index = index_, columns = columns_, values = values_).reset_index(col_level = 0)
    
    column_suffixes = data_pivot.columns.get_level_values(1)[-(len(values_*columns_values)):]
    column_names = data_pivot.columns.get_level_values(0)[:(len(values_*columns_values))-1].append(data_pivot.columns.get_level_values(0)[-(len(values_*columns_values)):] + '_' + column_suffixes)
    data_pivot.columns = column_names
    return data_pivot


def unit_to_toe(x_value, x_value_unit ,average_energy_content):
    
    """
        x_value_unit: string, indicating the unit of x_value
        average_energy_content: in MJ / unit indicated in x_value_unit
    """
    
    energy_toe = 41.868 #GJ/toe
    
    energy_value = x_value * average_energy_content
    x_value_gj_ = energy_value / 1000 #x value converted to GJ
    x_value_toe = x_value_gj_ / energy_toe
    
    #print(f'{x_value} {x_value_unit} of resource converted to toe with an average energy content of {average_energy_content} MJ/{x_value_unit}')
    return x_value_toe