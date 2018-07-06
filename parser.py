#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 27 11:42:46 2018

@author: joglas_souza
"""

import pandas as pd
import csv
from collections import defaultdict
from codes_descriptions import descriptions

#importing data as dataframe (I set a range of 282 columns because the source file is variable -- there aren't the same number of columns (tags) for each line )
# chr(1) == SOH
complete_dataset = pd.read_csv('NEW_xcbt_md_zc_fut.txt', header=None, sep=chr(1), names=list(range(0,282)))

#importing the lookup file (tags that needs to be considered)
tags_lookup = pd.read_csv('lookup_tags.txt', header=0)

#transforming the dataset in a list of dictionaries
list_of_dictionaries = []

#initializing dictionary that is going to be used to arrange the header of the file
header_dictionary = {} 
for tag in tags_lookup['tag_number']:
    header_dictionary[str(tag)] = 0

for i in range (len(complete_dataset)):
    line =  complete_dataset.iloc[i]   
    line_dictionary = {}
    count_duplicate_keys = 1
    
    #initializing dictionary that is going to be used to arrange the header of the file
    line_count_tags_dictionary = defaultdict() 
    
    for j in range (len(line)):  
        #spliting the tag in key values pairs
        key_value = line[j].split("=")
        
        # checking the case with multiple same tags in the same message
        if key_value[0] in line_dictionary:
            line_dictionary[key_value[0] + "_" + str(count_duplicate_keys)] = [key_value[1]] 
            count_duplicate_keys += 1
            line_count_tags_dictionary[key_value[0]] += 1
        else:
            line_dictionary[key_value[0]] = [key_value[1]]
            line_count_tags_dictionary[key_value[0]] = 1
        
        #stop when we reach the tag 10 (end of the message)
        if key_value[0] == '10':
            list_of_dictionaries.append(line_dictionary)
            break

    # updating the header dictionary with the number of times a tag is repeated on the line
    # the final number is going to be from the message that has the highest number of the same tag    
    for tag in line_count_tags_dictionary.keys():
        try:
            if header_dictionary[tag] < line_count_tags_dictionary[str(tag)]:
                header_dictionary[tag] = line_count_tags_dictionary[tag]
        except KeyError:
            pass

with open('parsed_file.csv', 'w') as fp:
    list_header = []
    
    #building the header of the file
    for i in range(len(tags_lookup)):
        number_of_same_tag = header_dictionary[str(tags_lookup['tag_number'][i])]
        
        for j in range(number_of_same_tag):
            if j == 0:
                list_header.append(tags_lookup['tag_name'][i])
            else:
                list_header.append(str(tags_lookup['tag_name'][i]) + "_" + str(j))
        
    a = csv.writer(fp, delimiter=",")
    # writing header on the file based on the list built above
    a.writerow(list_header)
     
    #iterating through the list of dictionary to get each one of the lines
    for i in range (len(list_of_dictionaries)):
        line = pd.DataFrame.from_dict(list_of_dictionaries[i], orient='columns')
        text_tag = ""
        number_of_commas = 0
        
        for j in range (len(tags_lookup)):           
                #adding descriptions to the codes for each tag (including the ones that have more than one for the same tag -- this is the reason we need a loop)
                for tag in line:
                    if tag == str(tags_lookup['tag_number'][j]) or tag.startswith(str(tags_lookup['tag_number'][j]) + "_"):
                        
                        #adding descriptions for the tags that requires one
                        if tags_lookup['need_description'][j] == 'y':
                            line[tag] = descriptions[tags_lookup['tag_name'][j]][line[tag][0]]                    
                        
                        # adding tag value to a string that is going to be used to write a csv file
                        text_tag = text_tag + str(line[tag][0]) + ","

                #adding a comma in case a tag does not exist on the message        
                if str(tags_lookup['tag_number'][j]) not in line:
                    text_tag = text_tag + " " + ","
                    
                #adding the correct number of commas (based on the biggest number of same tag)
                number_of_commas = number_of_commas + header_dictionary[str(tags_lookup['tag_number'][j])]
                difference_number_of_commas = number_of_commas - text_tag.count(',')
                if difference_number_of_commas > 0:
                    for i in range (difference_number_of_commas):
                        text_tag = text_tag + " " + ","
                
        #writing the rows on the file (desconsidering the last comma)        
        text_split = [x.strip() for x in text_tag[:-1].split(',')]    
        a.writerow(text_split)
fp.close()  
            