'''
@author: Reem Alatrash
@version: 1.0
=======================

This script compresses the folders created by the script "clean-copy-coha.py".
when running the script, if the paramter <del_folder>  is set to true, 
then folders are deleted after they've all been compressed.

'''

'''
******* ********* *********
*******  imports  *********
******* ********* *********
'''
import sys
import zipfile
import os
import shutil
from docopt import docopt
import logging

'''
******* ********* *********
******* variables *********
******* ********* *********
'''
# Get the arguments as global variables
args = docopt("""Extract contexts from COHA.

Usage:
	compress_del_folders.py <coha_dir> <del_folder> <output_dir>					
	
Arguments:       
	<coha_dir> = path to COHA directory
	<del_folder> = Remove folders after compression? Takes boolean values: T for True or  F for False.  
	<output_dir> = path to zipped output directory

""")

COHA_path = args['<coha_dir>']
del_folder = True if str(args['<del_folder>']).lower() == 't' else False 
output_path = args['<output_dir>']
# append an / to the end of given paths if it's missing
os.path.join(COHA_path, '')
os.path.join(output_path, '')

'''
******* ********* *********
******* functions *********
******* ********* *********
'''
def main():
	
	# ~ modified_tag_path = "modified/tagged/"
	# logging details
	my_format = "%(asctime)s - %(process)s - %(message)s"
	logging.basicConfig(filename="compress_log.txt", format=my_format, level=logging.INFO)
	logger = logging.getLogger()
	
	# ~ # get path to directories of clean tagged files
	# ~ mod_tagged_path = os.path.join(COHA_path, modified_tag_path)
	dir_file_names = os.listdir(COHA_path)
	# remove any zip files from the list
	dir_names = list(x for x in dir_file_names if ".zip" not in x)
	# start compressing folders
	logger.info("compressing folders in {}".format(COHA_path))
	for folder_name in dir_names:
		folder_path = os.path.join(COHA_path, folder_name)
		archive_name = "cleaned_{}.zip".format(folder_name)
		compress_files(folder_path, archive_name)
	# delete folder after compression
	if del_folder:
		# once all zip folders have been created, delete the uncompressed folders
		logger.info("deleting uncompressed folders")
		for folder_name in dir_names:
			folder_path = os.path.join(COHA_path, folder_name)
			shutil.rmtree(folder_path)		
	logger.info("done")		

def compress_files(folder_path, archive_name):
	'''Compress text files in given folder into 1 zip archive with the given name'''

	# check if given directory path exists
	if os.path.isdir(folder_path):
		# get list of files in directory
		txt_file_names = os.listdir(folder_path)
		# get destination (default to parent directory of folder if not output path is given)
		destination_path = output_path
		if not os.path.isdir(output_path):
			destination_path = os.path.split(folder_path)[0]
		# create path to output archive
		zip_path = os.path.join(destination_path,archive_name)
		# create archive
		with zipfile.ZipFile(zip_path, 'w') as myzip:
			for txt_file in txt_file_names:
				txt_file_path = os.path.join(folder_path,txt_file)
				# write file to archive without perserving the directory structure (arcname param)
				myzip.write(txt_file_path,arcname=txt_file)		

if __name__ == "__main__":
   main()			
