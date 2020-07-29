# CCOHA: Clean Corpus of Historical American English.
## About

This repository contains several Python scripts for creating a clean copy of the compressed tagged files of the COHA corpus. The resulting clean files are written in unicode (UTF-8).
The scripts in this repository are:

1. **clean-copy-coha.py:** Reads tagged (format: form \t lemma \t pos) files, which are compressed into zip folders, and creates clean copies of them under [COHA path]/modified/tagged/. 
All the clean files wihtin the same decades are saved under the same folder with that decade's name. For example, the folder [COHA path]/modified/tagged/1880s/ contains all the clean copies of files/documents between 1880-1889.
3. **generate_text_files.py:** Generates the text version of CCOHA by creating linear text files (paragraph style) from the new clean tagged files under [COHA path]/modified/text/. Text files of the same decade are aves under the same folder.
2. **compress_del_folders.py:** Compresses each decade folder into a ZIP archive and deletes the folder afterwards (optional). This has to be run twice, once after the tagged files are cleaned and once after the text files are generated.

##### How data is cleaned
For a description of the cleaning process, refer to the publication [CCOHA: Clean Corpus of Historical American English (Reem Alatrash et al. 2020)](https://www.aclweb.org/anthology/2020.lrec-1.859)

## Features

Main:
- Cleans most malformed tokens and provides new POS tags and lemmas using NLTK libraries.
- Handles empty and NUL values within tagged files.
- Unescapes HTML tokens and cleans them (POS tagging and Lemmatization).
- Rebuilds broken sentences resulting from malformed tokens.
- Provides sentence tokenization and adds a special END-Of-Sentence token after each sentence. 
- Uses universal file encoding (UTF-8) instead of the original windows encoding (cp1252).

Technical:
- Uses multiprocessing for faster processing speeds.  

## Structure
The scripts assume the following file structure for the data:


```
COHA
|
└─── tagged
|    | wlp_1810s_ksw.zip
|    | wlp_1820s_hsi.zip
|    | .
|    | .
|    | .
|    | wlp_2000s_iey.zip
|
└───clean
|   └─── tagged
|   |    | cleaned_1810s.zip
|   |    | .
|   |    | .
|   |    | .
|   |    | cleaned_2000s.zip
|   |
|   └─── text
```

## Usage
Run the python files via line commands or using any python IDE. The following commands illustrate

##### Cleaning tagged files
The script can be called using terminal or shell commands with the following arguments:

- <coha_dir> = path to COHA directory.
- <rm_Null> = Remove null tokens? Takes boolean values: T for True or  F for False.  
- <mal_pos> = pos for malformed tokens that are not valid words.
- <nul_sub> = lemma/pos replacement text for columns that are nul (unicode: \x00).

```bash
python clean-copy-coha.py <coha_dir> <rm_Null> <mal_pos> <nul_sub>
```

Example
```bash
python clean-copy-coha.py "/mount/resources/corpora/COHA/" "T" "<sub>" "<nul>"
```

This should be followed by a command to run the compression script in order to match the structure of the original COHA directory:
```bash
python compress_del_folders.py <coha_dir> <del_folder> <output_dir>
```
Example
```bash
python compress_del_folders.py "/mount/resources/corpora/COHA/clean/tagged/" "T" ""
```
which accepts the following arguments:

- <coha_dir> = path to COHA directory
- <del_folder> = Remove folders after compression? Takes boolean values: T for True or  F for False.
- <output_dir> = path to zipped output directory

##### Generating text files
The script can be called using terminal or shell commands with the following arguments:

- <coha_dir> = path to COHA directory.

```bash
python generate_text_files.py <coha_dir>
```

Example
```bash
python generate_text_files.py "/mount/resources/corpora/COHA/"
```

This should be followed by a command to run the compression script in order to match the structure of the original COHA directory:
```bash
python compress_del_folders.py <coha_dir> <del_folder> <output_dir>
```
Example
```bash
python compress_del_folders.py "/mount/resources/corpora/COHA/clean/text/" "T" ""
```

##### Note(s)
Make sure you compress the tagged files before generating the text files
