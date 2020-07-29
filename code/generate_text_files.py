'''
@author: Reem Alatrash
@version: 1.0
=======================

This script generates clean "text" files from the cleaned tagged files of the COHA corpus.

Example:
---------
 
the file "fic_1936_10080.txt" can be found under the directory COHA/clean/tagged/ in the wlp_1930s_ney.zip file.
The script reads this file, joins all token forms within it using space and creates a file with the same name "fic_1936_10080.txt"
under the directory COHA/clean/text/

'''

'''
******* ********* *********
*******  imports  *********
******* ********* *********
'''
import sys
import zipfile
sys.path.append('../modules/')
import os
from docopt import docopt
import multiprocessing
import logging
import time
import codecs
from multiprocessing_logging import install_mp_handler

'''
******* ********* *********
******* variables *********
******* ********* *********
'''
COHA_path = "/mount/resources/corpora/COHA/"
tagged_dir = "clean/tagged/"
text_path = "clean/text/"
    
# Get the arguments as global variables
args = docopt("""Extract contexts from COHA.

Usage:
    generate_text_files.py <coha_dir>                
    
Arguments:       
    <coha_dir> = path to zipped COHA directory

""")

COHA_path = args['<coha_dir>']
# append an / to the end of given paths if it's missing
os.path.join(COHA_path, '')
# create zip file path
zip_file_path = "{0}{1}".format(COHA_path,tagged_dir)

'''
******* ********* *********
******* functions *********
******* ********* *********
'''

def process_text(zip_file_name):
    ''' Processes all text files within a zip file'''
    #read this archive/zip file
    current_path = "{0}{1}".format(zip_file_path,zip_file_name) 
    
    logger = logging.getLogger()
    # 2nd column in zip archive is the decade it covers
    decade = zip_file_name.split("_")[1].replace(".zip", "")
    # make a directory with decade in order to save txt files there
    dir_path = "{0}{1}{2}".format(COHA_path, text_path,decade)
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)

    with zipfile.ZipFile(current_path, 'r') as current_zip:
        #get list of files inside this archive
        text_files_list = current_zip.namelist()    
        for text_file_name in text_files_list:
            logger.info("processing {}".format(text_file_name))         
            # extract genre and year from file name (e.g. fic_1817_8554.txt)
            # genre = file_details[0]
            # year = file_details[1]
            file_details = text_file_name.split("_")
            #read text file into memory and clean it
            first_line = ""
            is_first = True  
            #read text file into memory
            tokens = []
            lemmas = []
            pos = []   
            with current_zip.open(text_file_name, 'r') as lines:                
                # save each column into a list 
                tokens, lemmas, pos = zip(*list(line.decode('utf-8').split("\t") for line in lines))
                # first line special handling
                first_line = tokens[0]

            # skip tokens that contain < since they are either html tags or end-of-sentence markers
            # skip "q!" tokens 
            clean_tokens = list(token for token in tokens[1:] if token!= "q!" and "<" not in token)
            # rebuild the sentences from the tokens list
            text = " ".join(clean_tokens)                                                               
            try:
                # write result (free text) to a new text file
                # under clean/text/[decade]/
                out_file_name = "{0}{1}{2}/{3}".format(COHA_path, text_path,decade,text_file_name) 
                with codecs.open(out_file_name, 'w+') as out_file:
                    line1 = "{0}\n\n".format(first_line)
                    out_file.write(line1.encode('utf-8'))
                    out_file.write(text.encode('utf-8'))                        
            except:
                logger.info("ERROR | failed to write results to file: {}".format(text_file_name)) 
                raise           
                        
    return True
                        
def main():
    number_of_processes = 10
    
    # create multiprocessing logger
    my_format = "%(asctime)s - %(process)s - %(message)s"
    logging.basicConfig(filename="generate_text_log.txt", format=my_format, level=logging.INFO, mode='w')
    install_mp_handler()
    logger = logging.getLogger()
    
    # get list of zip files in directory
    logger.info("Getting names of zip files in directory: %s" %zip_file_path)
    dir_file_names = os.listdir(zip_file_path)
    zip_file_names = list(x for x in dir_file_names if ".zip" in x)
    
    logger.info("Creating pool and mapping processes")
    # create a pool of processes to process zip files simultaneously
    pool = multiprocessing.Pool(number_of_processes)
    # fire up the processes for our zip files
    result = pool.map(process_text, zip_file_names)
    # close pool after all requests are submitted
    pool.close()
    # wait for the last worker to finish, not needed when using map but I'd rather be safe than sorry 
    pool.join() 
    
    #done writing results to files
    logger.info("done")                                              

if __name__ == "__main__":
   main()
