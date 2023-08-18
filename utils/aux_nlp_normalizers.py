##-- Libraries and Packages
import pandas as pd
import re
import unicodedata
from fuzzywuzzy import fuzz
import Levenshtein
import nltk
from nltk.corpus import stopwords

##-- NLP - Normalizers
def nlp_preprocess(data, abrev_dict = None, stop_words_treatment = False, stop_words_lang = 'english', custom_stopwords = None):
    """
    nlp_preprocess doc:
        Function to preprocess / normalize strings (basic)
        returns an array
        
        data = pd.series
        abrev_dict = dictionary (in case of requires to deal with abreviations)   
    """                 
    
    def remove_stop_words(data):
        tokens = nltk.word_tokenize(data)
        filtered_tokens = [word for word in tokens if word.lower() not in stop_words]
        filtered_text = ' '.join(filtered_tokens)
        return filtered_text      
    
    #Capitalization
    data = data.str.lower()
    #Special Characters
    data = data.apply(lambda x: re.sub(r'[^\w\s]','',x).strip())
    #Abreviations
    if abrev_dict == None:
        pass
    else:
        data = data.apply(lambda x: ' '.join([abrev_dict[word] if word in abrev_dict else word for word in x.split(' ')]).strip())
    #Normalization
    data = data.apply(lambda x: unicodedata.normalize('NFKD',x).encode('ASCII','ignore').decode('utf-8').strip())
    #Stop words
    if stop_words_treatment:
        nltk.download('stopwords')
        nltk.download('punkt') #for tokenizing
        stop_words = set(stopwords.words(stop_words_lang)) 
        
        if custom_stopwords == None:
            pass
        else:
            for custom_stop in custom_stopwords:
                stop_words.add(custom_stop)
        
        data = data.apply(remove_stop_words)      
    #Multiple spaces
    data = data.str.replace(r'\s+',' ', regex = True)
    
    return data.values


def names_normalizer_fuzzy(x_input, y_compare, y_text, y_code = None, scaled = False):
    """
    multiple_matching_methods doc:
        Function to preprocess and normalize names using fuzzywuzzy.
        
        x_input: (pd.Series) Main column to compare with y_compare.
        
        y_compare: (pd.DataFrame) Should also contain the codes to match with x_input, where:
            y_text: (str) Column name of names to be compared.
            y_code: (str) Column name of codes corresponding to the matched names.
            
        Returns three arrays, the matched, the codes and the matching method used in each case
    """
    
    def min_max_scaler(data):
        min_val = min(data)
        max_val = max(data)
        
        scaled_data = [(x - min_val) / (max_val - min_val) for x in data]
        return scaled_data
    
    def strings_comparison(string_x, string_y):        
        return fuzz.ratio(string_x, string_y)    
    
    x_input = x_input.drop_duplicates().reset_index(drop = True)
    y_compare = y_compare.drop_duplicates().reset_index(drop = True)
    
    x_input_col_label_ = x_input.name
    distance_label_ = 'fuzzywuzzy'
    
    results = []
    
    for x_value in x_input:
        max_ratio = 0 #Initializing the variable, in the loop will be changed.
        matched_string = ""
        
        if y_code == None:        
            for y_value in y_compare[y_text]:
                ratio = strings_comparison(string_x = x_value, string_y = y_value)
                if ratio > max_ratio:
                    max_ratio = ratio
                    matched_string = y_value        
            results.append((x_value, matched_string, max_ratio)) 
            
        else:        
            for y_value, code in zip(y_compare[y_text], y_compare[y_code]):
                ratio = strings_comparison(string_x = x_value, string_y = y_value)
                if ratio > max_ratio:
                    max_ratio = ratio
                    matched_string = y_value
                    matched_code = code
        
            results.append((x_value, matched_string, matched_code, max_ratio))
        
    if y_code == None:
        results_df = pd.DataFrame(results, columns=[x_input_col_label_, f'matched_value_{distance_label_}',f'ratio_{distance_label_}'])
    else:
        results_df = pd.DataFrame(results, columns=[x_input_col_label_, f'matched_value_{distance_label_}',f'matched_code_{distance_label_}', f'ratio_{distance_label_}'])        
    
    if scaled:
        results_df[f'ratio_{distance_label_}_scaled'] = min_max_scaler(results_df[f'ratio_{distance_label_}'])
    
    results_df[f'target_ratio_{distance_label_}'] = 'max'
    
    return results_df



def names_normalizer_levenshtein(x_input, y_compare, y_text, y_code = None, scaled = False, weights=(1, 1, 1)):
    """
    multiple_matching_methods doc:
        Function to preprocess and normalize names using levenshtein distance.
        
        x_input: (pd.Series) Main column to compare with y_compare.
        
        y_compare: (pd.DataFrame) Should also contain the codes to match with x_input, where:
            y_text: (str) Column name of names to be compared.
            y_code: (str) Column name of codes corresponding to the matched names.
            
            weights: (insertion, deletion, substitution)
            
        Returns three arrays, the matched, the codes and the matching method used in each case
    """
    
    def min_max_scaler(data):
        min_val = min(data)
        max_val = max(data)
        
        scaled_data = [(x - min_val) / (max_val - min_val) for x in data]
        return scaled_data
    
    def strings_comparison(string_x, string_y):        
        return Levenshtein.distance(string_x, string_y, weights= weights)  #weights=(insertion, deletion, substitution)    
    
    x_input = x_input.drop_duplicates().reset_index(drop = True)
    y_compare = y_compare.drop_duplicates().reset_index(drop = True)
    
    x_input_col_label_ = x_input.name
    distance_label_ = 'levenshtein'
    
    results = []
    
    for x_value in x_input:
        min_ratio = 1000 #Initializing the variable, in the loop will be changed.
        matched_string = ""
        
        if y_code == None:        
            for y_value in y_compare[y_text]:
                ratio = strings_comparison(string_x = x_value, string_y = y_value)
                if ratio < min_ratio:
                    min_ratio = ratio
                    matched_string = y_value        
            results.append((x_value, matched_string, min_ratio)) 
            
        else:        
            for y_value, code in zip(y_compare[y_text], y_compare[y_code]):
                ratio = strings_comparison(string_x = x_value, string_y = y_value)
                if ratio < min_ratio:
                    min_ratio = ratio
                    matched_string = y_value
                    matched_code = code
        
            results.append((x_value, matched_string, matched_code, min_ratio))
    
    if y_code == None:
        results_df = pd.DataFrame(results, columns=[x_input_col_label_, f'matched_value_{distance_label_}',f'ratio_{distance_label_}'])
    else:
        results_df = pd.DataFrame(results, columns=[x_input_col_label_, f'matched_value_{distance_label_}',f'matched_code_{distance_label_}', f'ratio_{distance_label_}'])        
    
    if scaled:
        results_df[f'ratio_{distance_label_}_scaled'] = min_max_scaler(results_df[f'ratio_{distance_label_}'])
    
    results_df[f'target_ratio_{distance_label_}'] = 'min'
    
    return results_df


def names_normalizer_hamming(x_input, y_compare, y_text, y_code = None, scaled = False):
    """
    multiple_matching_methods doc:
        Function to preprocess and normalize names using hamming distance.
        
        x_input: (pd.Series) Main column to compare with y_compare.
        
        y_compare: (pd.DataFrame) Should also contain the codes to match with x_input, where:
            y_text: (str) Column name of names to be compared.
            y_code: (str) Column name of codes corresponding to the matched names.
            
        Returns three arrays, the matched, the codes and the matching method used in each case
    """
    
    def min_max_scaler(data):
        min_val = min(data)
        max_val = max(data)
        
        scaled_data = [(x - min_val) / (max_val - min_val) for x in data]
        return scaled_data
    
    def strings_comparison(string_x, string_y):        
        return Levenshtein.hamming(string_x, string_y)   
    
    x_input = x_input.drop_duplicates().reset_index(drop = True)
    y_compare = y_compare.drop_duplicates().reset_index(drop = True)
    
    x_input_col_label_ = x_input.name
    distance_label_ = 'hamming'
    
    results = []
    
    for x_value in x_input:
        min_ratio = 1000 #Initializing the variable, in the loop will be changed.
        matched_string = ""
        
        if y_code == None:        
            for y_value in y_compare[y_text]:
                ratio = strings_comparison(string_x = x_value, string_y = y_value)
                if ratio < min_ratio:
                    min_ratio = ratio
                    matched_string = y_value        
            results.append((x_value, matched_string, min_ratio)) 
            
        else:        
            for y_value, code in zip(y_compare[y_text], y_compare[y_code]):
                ratio = strings_comparison(string_x = x_value, string_y = y_value)
                if ratio < min_ratio:
                    min_ratio = ratio
                    matched_string = y_value
                    matched_code = code
        
            results.append((x_value, matched_string, matched_code, min_ratio))
    
    if y_code == None:
        results_df = pd.DataFrame(results, columns=[x_input_col_label_, f'matched_value_{distance_label_}',f'ratio_{distance_label_}'])
    else:
        results_df = pd.DataFrame(results, columns=[x_input_col_label_, f'matched_value_{distance_label_}',f'matched_code_{distance_label_}', f'ratio_{distance_label_}'])        
    
    if scaled:
        results_df[f'ratio_{distance_label_}_scaled'] = min_max_scaler(results_df[f'ratio_{distance_label_}'])
    
    results_df[f'target_ratio_{distance_label_}'] = 'min'
    
    return results_df


def names_normalizer_jaro_winkler(x_input, y_compare, y_text, y_code = None, scaled = False,prefix_weight=0.1):
    """
    multiple_matching_methods doc:
        Function to preprocess and normalize names using hamming distance.
        
        x_input: (pd.Series) Main column to compare with y_compare.
        
        y_compare: (pd.DataFrame) Should also contain the codes to match with x_input, where:
            y_text: (str) Column name of names to be compared.
            y_code: (str) Column name of codes corresponding to the matched names.
            
            prefix_weight (float, optional) – Weight used for the common prefix of the two strings. Has to be between 0 and 0.25. Default is 0.1.
            
        Returns three arrays, the matched, the codes and the matching method used in each case
    """
    
    def min_max_scaler(data):
        min_val = min(data)
        max_val = max(data)
        
        scaled_data = [(x - min_val) / (max_val - min_val) for x in data]
        return scaled_data
    
    def strings_comparison(string_x, string_y):        
        return Levenshtein.jaro_winkler(string_x, string_y, prefix_weight=prefix_weight)   
    
    x_input = x_input.drop_duplicates().reset_index(drop = True)
    y_compare = y_compare.drop_duplicates().reset_index(drop = True)
    
    x_input_col_label_ = x_input.name
    distance_label_ = 'jaro_winkler'
    
    results = []
    
    for x_value in x_input:
        min_ratio = 0 #Initializing the variable, in the loop will be changed.
        matched_string = ""
        
        if y_code == None:        
            for y_value in y_compare[y_text]:
                ratio = strings_comparison(string_x = x_value, string_y = y_value)
                if ratio > min_ratio:
                    min_ratio = ratio
                    matched_string = y_value        
            results.append((x_value, matched_string, min_ratio)) 
            
        else:        
            for y_value, code in zip(y_compare[y_text], y_compare[y_code]):
                ratio = strings_comparison(string_x = x_value, string_y = y_value)
                if ratio > min_ratio:
                    min_ratio = ratio
                    matched_string = y_value
                    matched_code = code
        
            results.append((x_value, matched_string, matched_code, min_ratio))
    
    if y_code == None:
        results_df = pd.DataFrame(results, columns=[x_input_col_label_, f'matched_value_{distance_label_}',f'ratio_{distance_label_}'])
    else:
        results_df = pd.DataFrame(results, columns=[x_input_col_label_, f'matched_value_{distance_label_}',f'matched_code_{distance_label_}', f'ratio_{distance_label_}'])        
    
    if scaled:
        results_df[f'ratio_{distance_label_}_scaled'] = min_max_scaler(results_df[f'ratio_{distance_label_}'])
    
    results_df[f'target_ratio_{distance_label_}'] = 'max'
    
    return results_df


def names_normalizer_ensembled(x_input, y_compare, y_text, y_code = None):
    
    scaled = True
    
    x_input = x_input.drop_duplicates().reset_index(drop = True)
    y_compare = y_compare.drop_duplicates().reset_index(drop = True)
    
    functions = [names_normalizer_fuzzy,names_normalizer_levenshtein,names_normalizer_hamming,names_normalizer_jaro_winkler]
    functions_max = [names_normalizer_fuzzy,names_normalizer_jaro_winkler]
    
    x_input_col_label_ = x_input.name
    
    df_normalized = pd.DataFrame(x_input,columns = [x_input_col_label_])
    
    for func in functions:
        df_temp = func(x_input = x_input, y_compare = y_compare, y_text = y_text, y_code = y_code, scaled = scaled)
        
        if func in functions_max:
            try:
                df_temp['ratio_fuzzywuzzy_scaled'] = 1 - df_temp['ratio_fuzzywuzzy_scaled']
            except:
                df_temp['ratio_jaro_winkler_scaled'] = 1 - df_temp['ratio_jaro_winkler_scaled']
                
        df_normalized = df_normalized.merge(df_temp, on = x_input_col_label_, how = 'left')    
    
    vals_columns = ['matched_value_fuzzywuzzy','matched_value_levenshtein','matched_value_hamming','matched_value_jaro_winkler']
    normalized_matrix = df_normalized[vals_columns].apply(lambda x: x.mode(), axis = 1)
    normalized_matrix['high_warning_flag'] =  normalized_matrix.apply(lambda x: sum(x.isna() == False), axis = 1) > 1
    df_normalized['value'] = normalized_matrix.iloc[:,[0]]
    df_normalized['high_warning_flag'] = normalized_matrix.iloc[:,[-1]]
    
    if y_code == None:
        df_normalized = df_normalized[[x_input_col_label_,'value']].copy()
    else:
        cods_columns = ['matched_code_fuzzywuzzy','matched_code_levenshtein','matched_code_hamming','matched_code_jaro_winkler']
        df_normalized['code'] = df_normalized[cods_columns].apply(lambda x: x.mode(), axis = 1).iloc[:,[0]]
        df_normalized = df_normalized[[x_input_col_label_,'code','value','high_warning_flag']].copy()
    
    return df_normalized