'''
@author: Reem Alatrash
@version: 1.0
=======================

This script creates a clean copy of the compressed tagged files of the COHA corpus.
This is done in the following manner:
1. Read original tagged files and clean malformed tokens.
2. POS tag and lemmatize the cleaned tokens.
3. Write the results (a clean copy of the file) into a txt file under a folder with the corresponding decade.

*Note: the code also logs processing information and errors to the file "clean_log.txt"

Example:
---------
 
the file "fic_1936_10080.txt" can be found under the directory COHA/tagged/ in the wlp_1930s_ney.zip file.
The script reads the txt file, cleans it and creates a file with the same name "fic_1936_10080.txt"
under the directory COHA/clean/tagged/1930s

'''

'''
******* ********* *********
*******  imports  *********
******* ********* *********
'''
import sys
sys.path.append('../modules/')
import zipfile
import os
from docopt import docopt
import multiprocessing
import HTMLParser
import logging
import time
import re # regex library
import nltk
# download nltk libraries that are needed for tokenization, tagging and lemmatization
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')
import codecs
from multiprocessing_logging import install_mp_handler

'''
******* ********* *********
******* variables *********
******* ********* *********
'''
COHA_path = "/mount/resources/corpora/COHA/"
tagged_dir = "tagged/"
modified_tag_path = "clean/tagged/"
malformed_chars = ['.', "'", '--',' ',':', ';', '*', '?','!']
valid_apostrophs = ["n't","'s","'m","'d","'ve","'ing", "'ll", "etc."]

# build regular expression to be used to detect malformed tokens
# exclude 1st two chars (. and ') since they are handled in the 2nd pass over the data
malformed_regex_pass1 = re.compile("({0})".format('|'.join(re.escape(item) for item in malformed_chars[2:])))
# 2nd pass consider all chars
malformed_regex_pass2 = re.compile("({0})".format('|'.join(re.escape(item) for item in malformed_chars)))
html_hex_regex = re.compile("(&\w+;|&#[0-9]+;)")
saute_forms = ["sauteed","sauted","saut","saute","sauteing","sautes","sauting"]

# initialize the punkt sentence tokenizer 
sentence_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
# add special abbreviations to prevent tokenization errors
extra_abbreviations = ['dr', 'vs', 'mr', 'mrs', 'prof', 'inc', 'i.e', 'e.g', 'p.k', 'c.c.f', 'm.c', 'etc',
                       'o.k', 'fr', 'acct', 'co', 'd.o.a', 'approx', 'ave', 'bros', 'sq', 'st', 'd.j']
# update the default abbreviations set _params.abbrev_types by adding the above abbreviations to it
sentence_tokenizer._params.abbrev_types.update(extra_abbreviations)

# add control chars for tokens where pos tag = "null"
'''
# ASCII device control characters that cause splitting errors:
horizontal tab, line feed, and carriage return
Read more here: https://www.w3schools.com/charsets/ref_html_ascii.asp
We added the chars non-breaking space and < to ignore white spaces and html tags like <> and <P>
'''
control_chars = ['&nbsp;', '&#10;', '&#13;', '&#09;', '<']
    
# Get the arguments as global variables
args = docopt("""Extract contexts from COHA.

Usage:
    clean-copy-coha.py <coha_dir> <rm_Null> <mal_pos> <nul_sub>                 
    
Arguments:       
    <coha_dir> = path to zipped COHA directory
    <rm_Null> = Remove null tokens? Takes boolean values: T for True or  F for False.  
    <mal_pos> = pos for malformed tokens that are not valid words
    <nul_sub> = lemma/pos replacement text for columns that are nul (unicode: \x00)

""")

COHA_path = args['<coha_dir>']
rmNull = True if str(args['<rm_Null>']).lower() == 't' else False 
mal_pos = args['<mal_pos>']
nul_sub = args['<nul_sub>']
# append an / to the end of given paths if it's missing
os.path.join(COHA_path, '')
# create zip file path
zip_file_path = "{0}{1}".format(COHA_path,tagged_dir)

'''
******* ********* *********
******* functions *********
******* ********* *********
'''

def contais_control_chars(token):
	'''returns true if token form contains unicode control characters'''
	contains_chars = False
	for char in control_chars:
		if char.lower() in token.lower():
			#skip this token
			contains_chars =  True
	return contains_chars
	
def get_wordnet_pos(treebank_tag):
    ''' returns WORDNET compliant POS tag '''
    # WordNet POS tags are: NOUN = 'n', ADJ = 's', VERB = 'v', ADV = 'r', ADJ_SAT = 'a'
    # Descriptions (c) https://web.stanford.edu/~jurafsky/slp3/10.pdf
    if treebank_tag.startswith('J') or treebank_tag == "PDT" or treebank_tag == "RP":
        return nltk.corpus.wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return nltk.corpus.wordnet.VERB
    elif treebank_tag.startswith('N'):
        return nltk.corpus.wordnet.NOUN
    elif treebank_tag.startswith('R') or treebank_tag == "IN" or treebank_tag == "EX":
        return nltk.corpus.wordnet.ADV  
    else:
        # As default pos in lemmatization is Noun
        return nltk.corpus.wordnet.NOUN

def get_claws7_pos(treebank_tag):
    '''maps nltk a treebank pos tag to CLAWS7'''
    # Create a map between Treebank (used by nltk pos tagger) and CLAWS7 tagsets
    # CLAWS7 tag description (c) http://ucrel.lancs.ac.uk/claws7tags.html
    nltk_to_claws7 = {
            'CC':'CC', # coordin. conjunction (and, but, or)  
            'CD':'MC', # cardinal number (one, two)             
            'DT':'DD', # determiner (a, the)                    
            'EX':'EX', # existential 'there' (there)           
            'FW':'FW', # foreign word (mea culpa)             
            'IN':'II', # preposition/sub-conj (of, in, by)   
            'JJ':'JJ', # adjective (yellow)                  
            'JJR':'JJR', # adj., comparative (bigger)          
            'JJS':'JJT', # adj., superlative (wildest)           
            'LS':'MC', # list item marker (1, 2, One)          
            'MD':'VM', # modal (can, should)                    
            'NN':'NN1', # noun, sing. or mass (llama)          
            'NNS':'NN2', # noun, plural (llamas)                  
            'NNP':'NP1', # proper noun, sing. (IBM)              
            'NNPS':'NP2', # proper noun, plural (Carolinas)
            'PDT':'DB', # predeterminer (all, both)            
            'POS':'GE', # possessive ending ('s ) 
            'PRP':'PRP', # personal pronoun (I, you, he) 
            # the general tag PRP does NOT exist in CLAWS7 which has a different tag for each case        
            'PRP$':'APPGE', # possessive pronoun (your, one's)    
            'RB':'RR', # adverb (quickly, never)            
            'RBR':'RRR', # adverb, comparative (faster)        
            'RBS':'RRT', # adverb, superlative (fastest)     
            'RP':'RP', # particle (up, off)
            'SYM':'Y', # symbol (+,%, &)
            # 'Y' is a general COHA tag for punctuation and other symbols. It does NOT exist in CLAWS7
            'TO':'TO', # "to" (to)
            'UH':'UH', # interjection (ah, oops)
            'VB':'VV0', # verb base form (eat)
            'VBD':'VVD', # verb past tense (ate)
            'VBG':'VVG', # verb gerund (eating)
            'VBN':'VVN', # verb past participle (eaten)
            'VBP':'VV0', # verb non-3sg pres (eat)
            'VBZ':'VVZ', # verb 3sg pres (eats)
            'WDT':'DDQ', # wh-determiner (which, that)
            'WP':'PNQ', # wh-pronoun (what, who)
            # the general tag PNQ does NOT exist in CLAWS7 which has a different tag for each case 
            'WP$':'PNQ', # possessive (wh- whose)
            'WRB':'RRQ', # wh-adverb (how, where)
            '$':'Y', #  dollar sign ($)
            '#':'Y', # pound sign (#)
            '"':'Y', # left quote (' or ")
            '"':'Y', # right quote (' or ")
            '(':'Y', # left parenthesis ([, (, {, <)
            ')':'Y', # right parenthesis (], ), }, >)
            ',':'Y', # comma (,)
            '.':'Y', # sentence-final punc (. ! ?)
            ':':'Y', # mid-sentence punc (: ; ... -)
            "''":'Y' # apostrophe punc
        }
    claws_tag = "" 
    # if the pos tag can be mapped, return claws tag,
    # else return the nltk pos tag unchanged
    if treebank_tag.upper() in nltk_to_claws7:
        claws_tag = nltk_to_claws7[treebank_tag.upper()]
    else:
        claws_tag = treebank_tag
    
    return claws_tag.lower()

def is_malformed(token, first_pass=True):
    '''checks if given string token contains chars corresponding to malformed tokens'''
    # check if token contains at least 1 alphabet
    alphabet_regex = re.compile("(?=.*[a-z])")
    contains_alphabet = re.search(alphabet_regex,token)
    matches = None
    if contains_alphabet:
        # first pass over data ignores . since we are looking at each token
        # 2nd pass considers only sentence boundaries so look for .
        if first_pass:
            malformed_regex = malformed_regex_pass1
        else:    
            malformed_regex = malformed_regex_pass2
        # use regex search to return a list of all malformed chars in token
        matches = re.findall(malformed_regex,token)  
        # skip valid apostrophe uses
        for pattern in valid_apostrophs:
            if pattern in token:
                matches = None         
    return matches  

def tag_sentence(full_sentence):
    '''tags an entire sentence'''    
    # convert our string sentence into a list
    sentence = full_sentence.split()
    # POS tag tokens
    tagged = nltk.pos_tag(sentence)
    return tagged

def lemmatize(token, tagged_sentence, token_idx):
    '''lemmatizes a token given its pos tag and position in a sentence'''
    logger = logging.getLogger()
    result = []
    # initialize word net lemmatizer
    wordnet_lemmatizer = nltk.stem.WordNetLemmatizer() 
    
    try:
        #do
    
        # given the position of the token (it's index), get the token and its corresponding pos tag
        # index is needed in case of multiple occurrences of the same token form in the sentence with different pos tags
        tagged_token = tagged_sentence[token_idx]
        wordnet_pos=get_wordnet_pos(tagged_token[1])
        if tagged_token[0] == "-" or tagged_token[0] == "--":
            lemma = tagged_token[0]
            claws_pos = "z"
        elif not tagged_token[0].isalpha():
             lemma = tagged_token[0]
        else:
           # use nltk post tags to lemmatize
           lemma = wordnet_lemmatizer.lemmatize(tagged_token[0], pos=wordnet_pos)       
        # ~ logger.info("inside lemmatize | current token and pos: {},{}".format(token,tagged_token[1]))
        if tagged_token[1] == "":
            tagged_token[1] = "nn"
        # convert pos tag to CLAWS7 set and format as pos_<sub>
        claws_pos = get_claws7_pos(tagged_token[1])
        pos = "{}_{}".format(claws_pos, mal_pos)
        # return updated token info
        return (tagged_token[0].encode('utf8'), lemma.encode('utf8'), pos.encode('utf8'))   
    except:
        logger.info("ERROR | Lemmatizing token: {} | idx: {} | Sent Length: {}".format(token,token_idx, len(tagged_sentence)))
        raise
   
def clean_malformed(token, first_pass=True,full_sentence=""):
    '''splits based on boundaries, tags pos and lemmatizes'''
    logger = logging.getLogger()
    #~ logger.info("inside clean malformed | current token: {}".format(token))
    result = []
    wordnet_lemmatizer = nltk.stem.WordNetLemmatizer() 
    
    try:
        #do    
        if first_pass:
            # split token based on malformed characters except .
            temp_split = re.split(malformed_regex_pass1, token)
            split_tokens = list(z for z in temp_split if z !='' and z != ' ')
            # add tokens to the results with a <temp> lemma and POS 
            # so that they're proprely tagged and lemmatized in the 2nd pass
            temp = "<temp>"
            for tok in split_tokens:
                result.append((tok.encode('utf8'), temp.encode('utf8'), temp.encode('utf8')))  
        else:
            # 2nd pass
            # split token based on all malformed characters including .
            temp_split = re.split(malformed_regex_pass2, token)
            split_tokens = list(z for z in temp_split if z !='' and z != ' ')       
            # POS tag tokens
            tagged = tag_sentence(full_sentence)
            #~ logger.info("split token into{}".format(split_tokens))
           
            # get the split and tagged tokens as a list
            tagged_split_tokens = list(tok for tok in tagged if tok[0] in split_tokens)
            
            # lemmatize then add to results
            for tok in tagged_split_tokens:
                # use nltk post tags to lemmatize
                if tok[1] == "":
                    tok[1] = "nn"
                nltk_pos=get_wordnet_pos(tok[1])
                lemma = wordnet_lemmatizer.lemmatize(tok[0], pos=nltk_pos)
                # convert pos tag to CLAWS7 set and format as pos_<sub>
                claws_pos = get_claws7_pos(tok[1])
                if tok[0] == "-" or tok[0] == "--":
                    claws_pos = "z"       
                pos = "{}_{}".format(claws_pos, mal_pos)
                # add token information to results  
                result.append((tok[0].encode('utf8'), lemma.encode('utf8'), pos.encode('utf8')))
                # ~ logger.info("token cleaned successfully") 
        # done processing, return results
        return result
    except:
        logger.info("ERROR| Current Full Sentence: {}".format(full_sentence))
        raise

def complete_sentence(tokens, current_sentence, full_sentence, final_results):
    '''completes the current sentence based on the full sentence using the given tokens. Returns completed sentence and current sentence with extra tokens'''
    logger = logging.getLogger()
    #~ logger.info("inside complete sent | tokens list: \n{}".format(tokens))
    completed = False

    try:
        for tok in tokens:          
            # add token to current sentence
            current_sentence.append(tok[0])
            partial_sent = " ".join(current_sentence).lower().strip().replace(" .",".")
            # add (token, lemma, pos) to final results by reference (i.e. no need to return final results)
            final_results.append((tok[0], tok[1], tok[2]))
            if not completed and partial_sent == full_sentence.lower().strip():
                # add end-of-sentence <eos> to final results by reference
                final_results.append(("<eos>".encode('utf8'),"<eos>".encode('utf8'),"<eos>".encode('utf8')))
                completed = True
                current_sentence = []
        #~ logger.info("sentence completed") 
        # return remaining tokens from the splitting tokens if they are not part of this sentence.
        #~ logger.info("leftover tokens:\n{}".format(current_sentence))
        return current_sentence
    except:
        logger.info("ERROR| inside complete Sentence with full sent: {}".format(full_sentence))
        raise       

def write_to_file(header, body, decade, text_file_name):
	'''Writes the cleanup results to file'''
	 try:
		 # write final results (cleaned text) to a new text file
         # under clean/tagged/[decade]/
         out_file_name = "{0}{1}{2}/{3}".format(COHA_path, modified_tag_path,decade,text_file_name) 
         with codecs.open(out_file_name, 'w+') as out_file:
			 out_file.write(header)
			 # skip results if they are emopty (i.e. file is empty)
			 if not body:
				 return True  
			 for line in results:
				 txt = "{0}\t{1}\t{2}\n".format(line[0], line[1].lower(),line[2].lower())
				 out_file.write(txt)
    except:
		logger.info("ERROR | failed to write results to file: {}".format(text_file_name)) 
        raise 
   return True              
    
def process_text(zip_file_name):
    ''' Processes all text files within a zip file'''
    #read this archive/zip file
    current_path = "{0}{1}".format(zip_file_path,zip_file_name) 
    nul_bytes= ['\x00','\00','\0']
    nul_regex = re.compile("({0})".format('|'.join(nul_bytes)))
    
    logger = logging.getLogger()
    # ~ logger.info("getting decade and creating folder")
    # 2nd column in zip archive is the decade it covers
    decade = zip_file_name.split("_")[1]
    # make a directory with decade in order to save txt files there
    dir_path = "{0}{1}{2}".format(COHA_path, modified_tag_path,decade)
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
    # create an HTMLparser to help decode html symbols
    h_parser = HTMLParser.HTMLParser()

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
            results = []
            first_line = ""
            is_first = True     
            with current_zip.open(text_file_name, 'r') as lines:                
                '''
                # *** first pass over tokens to read and clean them ***
                # *** handles: "null" pos tag, sautee lemma, escaped html, malformed tokens without "." and "'"
                '''
                for line in lines:
                    # first line special handling
                    if is_first:
                        first_line = line.decode('cp1252').encode('utf8')
                        for nul in nul_bytes:
                            first_line = first_line.replace(nul, nul_sub)
                        is_first = False
                    else:
                        
                        try:
                            # some tokens in COHA have no pos tag (a white space)
                            # we must replace these empty spaces using rstrip()
                            # using replace doesn't work, do not use it.                            
                            current_token_info = line.decode('cp1252').rstrip().split("\t")
                            if len(current_token_info) < 3:
                                current_token_info.append(nul_sub)    
                                
                            # check if null token with form <> or <P>
                            if rmNull and (current_token_info[2].lower() == "null"):
								if contais_control_chars(current_token_info[0].lower()):
									#skip this token
									continue
                            # skip lines where all fields are q!
                            if current_token_info[0].lower() == "q!":
								continue
                            encoded_tok = current_token_info[0].encode('utf8')
                            is_form_nul = re.search(nul_regex, encoded_tok)
                            if is_form_nul:
                                current_token_info[0] = nul_sub
                            
                            
                            if current_token_info[0].lower() in saute_forms:
                                # unify lemma
                                current_token_info[1] = "saute"
                            # decode html in token  (if html is detected)
                            contains_html = re.search(html_hex_regex, current_token_info[0].lower())
                            if contains_html:
                                # ~ logger.info("{} contains html".format(current_token_info[0]))
                                current_token_info[0] = h_parser.unescape(current_token_info[0])
                
                            split_tokens = []    
                            # check if malformed token and fix it
                            is_malformed_matches = is_malformed(current_token_info[0].lower())
                            if is_malformed_matches and current_token_info[0].lower()!= "q!":
                                split_tokens = clean_malformed(current_token_info[0])
                            
                            if len(split_tokens) > 1:
                                results.extend(split_tokens)
                            else:
                                # no cleaning needed
                                results.append(current_token_info)             
                        except:
                            logger.info("ERROR during 1st pass over line {} in file {}".format(line, text_file_name))   
                            raise             
           
            '''
            # *** 2nd pass over tokens to clean them and define sentence boundaries ***
            # *** handles: empty lemmas and pos tags, adding sentence boundaries, malformed tokens with "." and "'" around sentence boundaries.
            '''
            # if results are empty (file is empty), write only first line and exit
            if not results:
				write_to_file(first_line, results, decade, text_file_name)				
            
            tokens = []
            lemmas = []
            pos = []        
            # save each column of our results into a list
            tokens, lemmas, pos = zip(*results) 
            # ~ logger.info("successfully unpacked results into 3 lists: tokens, lemmas, pos")
            # reset resutls in order to add sentence boundaries where needed
            results = []                        
            # rebuild the sentences from the tokens list
            text = " ".join(tokens)
            # use a sentence tokenizer to get a list of all the sentences in the file
            sentences_list = sentence_tokenizer.tokenize(text.strip())
            # ~ logger.info(len(sentences_list))      
            # variable to which we append our tokens to recreate the sentence 
            # based on boundaries set using the NLTK sentence tokenizer
            current_sentence = []
            # variable for the tagged full sentence, to be used for tokens cleaned in the 1st pass
            tagged_sentence = []
            # index of the current current full sentence
            sent_idx = 0
            # index of toekn within the current full sentence
            token_idx = -1
            # start processing tokens
            try:
                #do sth
                for idx in range(0,len(tokens)):
                    
                    # If the sentence index has exceeded the length of sentences, exit loop
                    if sent_idx == len(sentences_list):
                        break
                    token_idx += 1    
                    token_form = tokens[idx]
                    full_sentence = sentences_list[sent_idx]
                    # add to current partial sentence
                    current_sentence.append(token_form)
                    
                    # encode token info using utf-8
                    encoded_tok = token_form.encode('utf8')
                    enc_lem = lemmas[idx].encode('utf8')
                    enc_pos = pos[idx].encode('utf8')
                    
                    # replace NUL characters (white spaces) in lemma column
                    is_lemma_nul = re.search(nul_regex, enc_lem)
                    if is_lemma_nul:
                        for nul in nul_bytes:
                            enc_lem = enc_lem.replace(nul,nul_sub)

                    # add token info to final results     
                    results.append((encoded_tok,enc_lem,enc_pos))
                    
                    # compare current partial sentence to full sentence to detect end of sentence
                    partial_sentence = " ".join(current_sentence).lower().strip() 
                    is_eos = False # end of sentence marker 
                    # check if end of sentence
                    if partial_sentence ==  full_sentence.lower().strip():
						is_eos = True
                    
                    # check if current token was cleaned in 1st pass but not tagged and lemmatized
                    # skip tokens were the form is @ (special replacement token added by COHA creators for legal reasons)
                    # skip tokens that contain "." since those will be handled by the sentence boundary code block
                    prev_cleaned = (lemmas[idx] == "<temp>" or enc_lem == nul_sub or enc_pos == nul_sub)
                    needs_tag_lemma = (prev_cleaned and token_form != "@" and not is_eos)
                    if needs_tag_lemma:
                        # tag and lemmatize the current token based on its pos and position in sentence
                        tagged_sentence = tag_sentence(full_sentence)
                        # lemmatize token
                        if token_idx < len(tagged_sentence):
                            new_token_info = lemmatize(encoded_tok, tagged_sentence, token_idx)
                        else:
                            if prev_cleaned:
                                new_token_info = (encoded_tok, encoded_tok, enc_pos)    
                        # replace the token info in the final results
                        del results[-1]
                        results.append(new_token_info)
                        # ~ logger.info("{} was tagged and lemmatized successfully.".format(tokens[idx]))    
                              
                    # check if end of sentence    
                    if is_eos:
                        # end of sentence reached, reset current partial sentence
                        current_sentence = []
                        # reset the tagged full sentence
                        tagged_sentence = []
                        # add eos token to the final results
                        results.append(("<eos>".encode('utf8'),"<eos>".encode('utf8'),"<eos>".encode('utf8')))
                        token_idx = -1
                        sent_idx += 1                                    
                    else:
                        # check if current sentence has passed the boundaries of the full sentence
                        if len(partial_sentence) > len(full_sentence):
                            # check if malformed token and fix it
                            is_malformed_matches = is_malformed(token_form, False)
                            if is_malformed_matches and token_form.lower()!= "q!":
                                # malformed token found
                                # 1. remove malformed token from the current sentence and final results then split it
                                del current_sentence[-1]
                                del results[-1]
                                split_tokens = clean_malformed(token_form, False, full_sentence)
                                #~ logger.info("split_tokens \n {}".format(split_tokens))
                                # 2. add the split tokens until sentence is complete and reset the current sentence
                                current_sentence = complete_sentence(split_tokens, current_sentence, full_sentence, results)
                                # 3. we completed the sentence so reset the tagged sentence
                                # 3.2 reset the tagged full sentence and token index
                                tagged_sentence = []
                                token_idx = -1
                                # 3.3 get next full sentence by moving the iterator
                                sent_idx += 1
            except:
                logger.info("ERROR| Current Sentence: {}".format(current_sentence))
                raise                         
                                                                                            
            write_to_file(first_line, results, decade, text_file_name)   
                        
    return True
                        
def main():
    number_of_processes = 10
    
    # create multiprocessing logger
    my_format = "%(asctime)s - %(process)s - %(message)s"
    logging.basicConfig(filename="clean_log.txt", format=my_format, level=logging.INFO, mode='w')
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
