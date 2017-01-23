# -*- coding: utf-8 -*-

import urllib2
import subprocess
from KafNafParserPy import KafNafParser
import os
import re
import urllib
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib2 import Request, urlopen, URLError
import json
import time
import sys, getopt
import operator
import codecs
from ConfigParser import SafeConfigParser

config = SafeConfigParser()
config.read('config.ini')
pikes_url = config.get('config', 'pikes_url')
nominatim_url = config.get('config', 'nominatim_url')
dbpedia_url = config.get('config', 'dbpedia_url')
delay_db = float(config.get('config', 'delay_db'))
delay_nominatim = float(config.get('config', 'delay_nominatim'))


use_pantheon_file = False
use_more_chains = False
try:
    opts, args = getopt.getopt(sys.argv[1:], "p:el:o:",["","",""])
except getopt.GetoptError:
    sys.exit(2)
for opt, arg in opts:
    if opt == '-p':
        pantheon_data_file = arg
        use_pantheon_file=True
    if opt == '-e':
        use_more_chains=True
    if opt == '-l':
        list_file = arg
    if opt == '-o':
        movements_output_file = arg


def wiki_bio_download (list_file_name, out_dir):
    for name in codecs.open(list_file_name, 'r',  "utf-8"):
        name=name.rstrip('\n')
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        if not os.path.exists(out_dir+"/"+name+".html"):
            url="https://en.wikipedia.org/wiki/"+name
            sys.stdout.write("    Downloading "+name+"\n")
            url = urllib2.quote(url.encode('utf8'), ':/')
            response = urllib2.urlopen(url)
            html = response.read()
            html = html.decode('utf8')
            outname = out_dir+"/"+name+".html"
            html_out = codecs.open(outname, 'w',  "utf-8")
            html_out.write(html)
    sys.stdout.write("All files downloaded\n")

def clean_wiki_pages(list_file_name, html_files_dir, out_dir):
    for name in codecs.open(list_file_name, 'r', "utf-8"):
        name = name.rstrip('\n')
        path = html_files_dir+"/"+name+".html"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        outname = out_dir + "/" + name + ".txt"
        if not os.path.exists(outname):
            txt_out = codecs.open(outname, 'w', "utf-8")
            p = subprocess.Popen(['perl', 'wiki_cleaner.pl', path], stdout=subprocess.PIPE, stderr = subprocess.PIPE)
            clean_text, err = p.communicate()
            clean_text = clean_text.decode('utf8')
            txt_out.write(clean_text)
    sys.stdout.write("All files converted to text\n")

def txt_to_naf(list_file_name, txt_files_dir, out_dir):
    sys.stdout.write('Converting texts to .naf files (each file will take some minutes, depending on its length):\n')

    #for name in codecs.open(list_file_name, 'r', "utf-8"):
    for name in open(list_file_name):
        document = u''
        name = name.rstrip('\n')
        txt_path = txt_files_dir + "/" + name + ".txt"
        naf_path = out_dir + "/" + name + ".naf"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        if not os.path.exists(naf_path):
            sys.stdout.write("    "+name + "->.naf\n")
            for line in codecs.open(txt_path, 'r', "utf-8"):
                line = line.rstrip('\n')
                document=document+" "+line
            document = urllib2.quote(document.encode('utf8'), ':/')
            #data="text="+document
            data="meta_title="+name+"&meta_filename="+name+"&text="+document
            req = urllib2.Request(pikes_url, data)
            response = urllib2.urlopen(req)
            naf = response.read()
            response = naf.decode('utf8')
            outname = out_dir + "/" + name + ".naf"
            # outname=outname.decode("utf-8")
            naf_out = codecs.open(outname, 'w', "utf-8")
            naf_out.write(response)
    sys.stdout.write("All files converted to .naf\n")


def tab_to_json(txt_file, json_file):
    java_out = subprocess.Popen(['java', '-jar', 'PantheonMovementsJSON.jar', txt_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    json_to_print, err = java_out.communicate()
    json_output = open(json_file, 'w')
    json_output.write(json_to_print)
    json_output.close()
    sys.stdout.write("Movements exported to .json\n")


def clean_na (file_mov, file_clean):
    output_file = open(file_clean, 'w')


    for line in codecs.open(file_mov, 'r', "utf-8"):
        splitted_line = line.split('\t')
        lat = splitted_line[14]
        lat=re.sub(r'lat\: ', '', lat)
        lon = splitted_line[15]
        lon = re.sub(r'lon\: ', '', lon)
        if splitted_line[11]!="NA" and lat!="NA" and lon!="NA":
            output_file.write(line.encode("utf-8"))
    output_file.close()

def extract_movements (naf_folder,list_file, movements_output_file, use_pantheon_file,pantheon_data_file,use_more_chains):
    sys.stdout.write('Extracting movements from .naf files:')
    out_movements_file = open(movements_output_file,"w")
    n_done=1
    dict_nation = dict()
    dict_profession = dict()
    dict_group = dict()
    dict_area = dict()
    dict_gender = dict()
    dict_continent = dict()
    set_person_list= set()

    for line in codecs.open(list_file, 'r', "utf-8"):
        set_person_list.add(line.rstrip('\n'))

    if use_pantheon_file==True:
        for line in open(pantheon_data_file):
            splitted_line = line.split('\t')
            splitted_line[1] = re.sub(r' ', '_', splitted_line[1])
            dict_nation[splitted_line[1]] = splitted_line[6]
            dict_profession[splitted_line[1]] = splitted_line[11]
            dict_group[splitted_line[1]] = splitted_line[12]
            dict_area[splitted_line[1]] = splitted_line[13]
            dict_gender[splitted_line[1]] = splitted_line[10]
            dict_continent[splitted_line[1]] = splitted_line[9]

    nottoinclude = re.compile('[a-zA-Z]+|\*{4}')

    def role_filter(FrameName , Rolename, role_naf_object , role_span_naf_object ,mapping_t_to_w , list_w_in_coref):
        return_val = 0
        rol_ext_ref = role_naf_object.get_external_references()
        for pred_ext_ref in rol_ext_ref:
            if FrameName in pred_ext_ref.get_resource():
                semantic_role = pred_ext_ref.get_reference()
                sem_rol_split = semantic_role.split("@")
                if sem_rol_split[1] == Rolename:
                    for rol_id in role_span_naf_object:
                        for i in mapping_t_to_w[rol_id.get_id()]:
                            if i in list_w_in_coref:
                                return_val = 1
        return return_val

    def remove_duplicates(values):
        output = []
        seen = set()
        for value in values:
            # If value has not been encountered yet,
            # ... add it to both list and set.
            if value not in seen:
                output.append(value)
                seen.add(value)
        return output



    def birth_death (name, sparql_url):
        # extract from dbpadia date and place of birth and death
        # spaql url can be set in the config file
        delay_db = 1
        name = re.sub(r' ', '_', name)
        query_birth_city_dbp = "prefix dbo: <http://dbpedia.org/ontology/> prefix georss: <http://www.georss.org/georss/> select ?birthPlace ?coords where {<http://dbpedia.org/resource/" + name + "> <http://dbpedia.org/ontology/birthPlace> ?birthPlace .FILTER (EXISTS {?birthPlace rdf:type dbo:Town} || EXISTS {?birthPlace rdf:type dbo:City}) .?birthPlace georss:point ?coords .}"
        query_death_city_dbp = "prefix dbo: <http://dbpedia.org/ontology/> prefix georss: <http://www.georss.org/georss/> select ?deathPlace ?coords where {<http://dbpedia.org/resource/" + name + "> <http://dbpedia.org/ontology/deathPlace> ?deathPlace .FILTER (EXISTS {?deathPlace rdf:type dbo:Town} || EXISTS {?deathPlace rdf:type dbo:City}) .?deathPlace georss:point ?coords .}"
        query_birth_settlement_dbp = "prefix dbo: <http://dbpedia.org/ontology/> prefix georss: <http://www.georss.org/georss/> select ?birthPlace ?coords where {<http://dbpedia.org/resource/" + name + "> <http://dbpedia.org/ontology/birthPlace> ?birthPlace .?birthPlace georss:point ?coords .}"
        query_death_settlement_dbp = "prefix dbo: <http://dbpedia.org/ontology/> prefix georss: <http://www.georss.org/georss/> select ?deathPlace ?coords where {<http://dbpedia.org/resource/" + name + "> <http://dbpedia.org/ontology/deathPlace> ?deathPlace .?deathPlace georss:point ?coords .}"
        query_birth_date = "select ?birthDate where{<http://dbpedia.org/resource/" + name + "> <http://dbpedia.org/ontology/birthDate> ?birthDate}"
        query_death_date = "select ?deathDate where{<http://dbpedia.org/resource/" + name + "> <http://dbpedia.org/ontology/deathDate> ?deathDate}"
        city_b_found = 0
        city_d_found = 0
        location_b = "NA"
        location_d = "NA"
        year_b = "NA"
        year_d = "NA"

        sparql = SPARQLWrapper(sparql_url)
        sparql.setReturnFormat(JSON)

        if city_b_found == 0:
            time.sleep(delay_db)
            sparql.setQuery(query_birth_city_dbp)
            try:
                results = sparql.query().convert()
                for result in results['results']['bindings']:
                    if city_b_found == 0:
                        location_b = result['birthPlace']['value']
                        location_b = re.sub(r'http://dbpedia.org/resource/', '', location_b)
                        city_b_found = 1
            except URLError, e:
                print 'error', e

        if city_b_found == 0:
            time.sleep(delay_db)
            sparql.setQuery(query_birth_settlement_dbp)
            try:
                results = sparql.query().convert()
                for result in results['results']['bindings']:
                    location_b = result['birthPlace']['value']
                    location_b = re.sub(r'http://dbpedia.org/resource/', '', location_b)
                    city_b_found = 1
            except URLError, e:
                print 'error', e

        if city_d_found == 0:
            time.sleep(delay_db)
            sparql.setQuery(query_death_city_dbp)
            try:
                results = sparql.query().convert()
                for result in results['results']['bindings']:
                    if city_d_found == 0:
                        location_d = result['deathPlace']['value']
                        location_d = re.sub(r'http://dbpedia.org/resource/', '', location_d)
                        city_d_found = 1
            except URLError, e:
                print 'error', e

        if city_d_found == 0:
            time.sleep(delay_db)
            sparql.setQuery(query_death_settlement_dbp)
            try:
                results = sparql.query().convert()
                for result in results['results']['bindings']:
                    location_d = result['deathPlace']['value']
                    location_d = re.sub(r'http://dbpedia.org/resource/', '', location_d)
                    city_d_found = 1
            except URLError, e:
                print 'error', e

        time.sleep(delay_db)
        sparql.setQuery(query_birth_date)
        try:
            results = sparql.query().convert()
            for result in results['results']['bindings']:
                year_b = result['birthDate']['value']
                year_b = re.sub(r'http://dbpedia.org/resource/', '', year_b)
                # year_b = re.sub(r'-', '', year_b)
                year_b = year_b.replace('-', '')
        except URLError, e:
            print 'error', e

        time.sleep(delay_db)
        sparql.setQuery(query_death_date)
        try:
            results = sparql.query().convert()
            for result in results['results']['bindings']:
                year_d = result['deathDate']['value']
                year_d = re.sub(r'http://dbpedia.org/resource/', '', year_d)
                # year_d = re.sub(r'-', '', year_d)
                year_d = year_d.replace('-', '')
        except URLError, e:
            print 'error', e

        return (location_b,year_b,location_d,year_d)


    def georeference (location_string, nominatim_url):
        # query to nominatim for the coordinates of a place
        lat="NA"
        lon="NA"

        try:
            location_api = urllib.quote(location_string)
            url_streetmap = nominatim_url + "/search.php?q=" + location_api + "&format=json"
            time.sleep(delay_nominatim)
            request_s = Request(url_streetmap)
            try:
                response_s = urlopen(request_s)
                json_response_s = response_s.read()
                js_s = json.loads(json_response_s)
                if js_s:
                    if js_s[0]['type'] != 'locality':
                        lat = js_s[0]['lat']
                        lon = js_s[0]['lon']
                else:
                    location_temp = ''
                    if ' ' in location_string:
                        clean_location_string = location_string.split(' ')
                        for w in clean_location_string:
                            if w[0].isupper():
                                location_temp = location_temp + w + ' '
                            else:
                                location_temp = location_temp + '\t'

                        location_temp = re.sub(r' \t', '\t', location_temp)
                        location_temp = re.sub(r' $', '', location_temp)
                        splitted_location = location_temp.split('\t')

                        for word in splitted_location:
                            location_api = urllib.quote(word)
                            url_streetmap = nominatim_url + "/search.php?q=" + location_api + "&format=json"
                            time.sleep(delay_nominatim)
                            request_s = Request(url_streetmap)
                            try:
                                response_s = urlopen(request_s)
                                json_response_s = response_s.read()
                                js_s = json.loads(json_response_s)
                                if js_s:
                                    if js_s[0]['type'] != 'locality':
                                        lat = js_s[0]['lat']
                                        lon = js_s[0]['lon']
                                else:
                                    if ' ' in word:
                                        word_loc_list = word.split(' ')
                                        for w in word_loc_list:
                                            location_api = urllib.quote(w)
                                            url_streetmap = nominatim_url + "/search.php?q=" + location_api + "&format=json"
                                            time.sleep(delay_nominatim)
                                            request_s = Request(url_streetmap)
                                            try:
                                                response_s = urlopen(request_s)
                                                json_response_s = response_s.read()
                                                js_s = json.loads(json_response_s)
                                                if js_s:
                                                    if js_s[0]['type'] == 'city' \
                                                            or js_s[0]['type'] == "village" \
                                                            or js_s[0]['type'] == "town":
                                                        lat = js_s[0]['lat']
                                                        lon = js_s[0]['lon']
                                                    else:
                                                        if lat == '':
                                                            lat = "NA"
                                                            lon = "NA"
                                            except URLError, e:
                                                print 'error1', e

                                        if lat == "NA":
                                            banned_words = ['Academy', 'Army', 'Association', 'Barracks', 'Cemetery', 'Center',
                                                            'Central', 'Church', 'Club', 'Clubs', 'College', 'Committee',
                                                            'Communion', 'Conference', 'Congregation', 'Congress',
                                                            'Corporation', 'Farm', 'Foundation', 'Front', 'Governorate',
                                                            'Headquarters', 'High', 'House', 'Hut', 'Imperial', 'Institute',
                                                            'Institution', 'Laboratory', 'League', 'Magazine', 'Ministry',
                                                            'Mountains', 'Museum', 'National', 'Nursery', 'Observatory',
                                                            'Office', 'Park', 'Radio', 'Records', 'Regia', 'Royal', 'School',
                                                            'Schools', 'Scuola', 'Service', 'Society', 'Tournament',
                                                            'University']
                                            word_loc_list = word.split(' ')
                                            for w in word_loc_list:
                                                if w not in banned_words:
                                                    location_api = urllib.quote(w)
                                                    url_streetmap = nominatim_url + "/search.php?q=" + location_api + "&format=json"
                                                    time.sleep(delay_nominatim)
                                                    request_s = Request(url_streetmap)
                                                    try:
                                                        response_s = urlopen(request_s)
                                                        json_response_s = response_s.read()
                                                        js_s = json.loads(
                                                            json_response_s)
                                                        if js_s:
                                                            if js_s[0]['type'] != 'locality':
                                                                if js_s[0]['class'] == 'boundary':
                                                                    lat = js_s[0]['lat']
                                                                    lon = js_s[0]['lon']
                                                                else:
                                                                    if lat == '':
                                                                        lat = "NA"
                                                                        lon = "NA"
                                                    except URLError, e:
                                                        print 'error2', e
                                        else:
                                            if lat == '':
                                                lat = "NA"
                                                lon = "NA"
                            except URLError, e:
                                print 'error3', e
            except URLError, e:
                print 'error4', e
        except URLError, e:
            print 'error5', e

        return(lat,lon)

    for file in os.listdir(naf_folder):
        file_found=0

        if file.endswith(".naf"):

            list_check=file
            list_check = re.sub(r'\.naf', '', list_check)
            if file==file:
                my_parser = KafNafParser(naf_folder+"/"+file)
                movements_found = 0
                movements_number=0
                dict_timex = dict()
                dict_loc = dict()
                dict_loc_full = dict()
                dict_sent = dict()
                list_coref_w = []
                dict_w_to_t = dict()
                dict_t_to_w = dict()
                dict_t_to_lemma = dict()

                for term_obj in my_parser.get_terms():
                    for w_in_term in term_obj.get_span().get_span_ids():
                        dict_w_to_t[w_in_term] = []
                        dict_w_to_t[w_in_term].append(term_obj.get_id())
                        dict_t_to_w[term_obj.get_id()] = []
                        dict_t_to_w[term_obj.get_id()].append(w_in_term)

                dic_words = dict()
                for token_obj in my_parser.get_tokens():
                    dic_words[token_obj.get_id()] = []
                    dic_words[token_obj.get_id()].append(token_obj.get_text())

                dic_word_to_sentence = dict()
                for token_obj_2 in my_parser.get_tokens():
                    dic_word_to_sentence[token_obj_2.get_id()] = []
                    dic_word_to_sentence[token_obj_2.get_id()].append(token_obj_2.get_sent())

                dic_sentence_to_words = dict()

                for token_obj_3 in my_parser.get_tokens():
                    if token_obj_3.get_sent() in dic_sentence_to_words:
                        dic_sentence_to_words[token_obj_3.get_sent()].append(token_obj_3.get_id())
                    else:
                        dic_sentence_to_words[token_obj_3.get_sent()] = []
                        dic_sentence_to_words[token_obj_3.get_sent()].append(token_obj_3.get_id())

                for term_obj in my_parser.get_terms():
                    for idlemma in term_obj.get_span().get_span_ids():
                        for t in dict_w_to_t[idlemma]:
                            dict_t_to_lemma[t] = term_obj.get_lemma()

                # Read file name from .naf metadata
                file_name = my_parser.get_header().get_fileDesc().get_filename()
                enc_file_name = file_name.encode("utf8")

                # extract birth and death info from dbpedia
                dbpedia_target = enc_file_name
                dbpedia_target = re.sub(r'\.txt', '', dbpedia_target)
                dbpedia_target = re.sub(r' ', '_', dbpedia_target)
                (location_b, year_b, location_d, year_d) = birth_death(dbpedia_target, dbpedia_url)
                location_b=location_b.encode("utf8")
                location_b = re.sub(r'_', ' ', location_b)
                (lat_b, lon_b) = georeference(location_b, nominatim_url)
                location_d = location_d.encode("utf8")
                location_d = re.sub(r'_', ' ', location_d)
                (lat_d, lon_d) = georeference(location_d, nominatim_url)

                # looks for entities matching with the name of the page
                sys.stdout.write('\n')
                sys.stdout.write('    '+str(n_done)+'\t'+enc_file_name+'\t')
                n_done = n_done + 1
                enc_file_name = re.sub(r'\.txt', '', enc_file_name)
                enc_file_name = re.sub(r' ', '_', enc_file_name)
                set_t_sbj_match = set()
                set_t_sbj_exact_match = set()
                set_t_sbj_match_temp = set()
                set_exact_match_entities_id_lists=set()

                for entity_obj in my_parser.get_entities():

                    string_to_print = ''
                    w_id_to_print = ''
                    entity_type = entity_obj.get_type()

                    if entity_type == 'PERSON':
                        for entity_ref in entity_obj.get_references():
                            entity_span = entity_ref.get_span()
                            entity_span_list_id = entity_span.get_span_ids()
                            entity_span_list_temp= set()
                            for item in entity_span_list_id:
                                entity_span_list_temp.add(item)
                                set_t_sbj_match_temp.add(item)  # save the id of the person matching the subject of the biography
                                w_id_to_print = w_id_to_print + ' ' + item
                                for word_id in dict_t_to_w[item]:
                                    stringa = my_parser.get_token(word_id).get_text()
                                    enc_stringa = stringa.encode("utf8")
                                    string_to_print = string_to_print + ' ' + enc_stringa

                            string_to_print=string_to_print.lstrip()
                            enc_file_name = re.sub(r'_', ' ', enc_file_name)
                            enc_file_name = re.sub(r'.txt', '', enc_file_name)

                            if string_to_print in enc_file_name:
                                if string_to_print == enc_file_name and enc_file_name==string_to_print:
                                    set_exact_match_entities_id_lists.add(tuple(sorted(entity_span_list_temp)))
                                    set_t_sbj_exact_match = set_t_sbj_exact_match.union(set_t_sbj_match_temp)
                                set_t_sbj_match = set_t_sbj_match.union(set_t_sbj_match_temp)
                                set_t_sbj_match_temp = set()
                                break
                            else:
                                set_t_sbj_match_temp = set()

                # find the coreference to use
                dict_matching_coreferences = dict()
                dict_exact_matching_coreferences = dict()
                for coref_obj in my_parser.get_corefs():
                    span_counter = 0
                    for span_item in coref_obj.get_spans():
                        span_counter += 1
                    for coref_span in coref_obj.get_spans():
                        coref_span_id = coref_span.get_span_ids()
                        for c_id in coref_span_id:
                            if c_id in set_t_sbj_match:
                                coref_subject = coref_obj.get_id()
                                dict_matching_coreferences[coref_subject] = []
                                dict_matching_coreferences[coref_subject].append(span_counter)
                        if tuple(coref_span_id) in set_exact_match_entities_id_lists:
                            coref_subject = coref_obj.get_id()
                            dict_exact_matching_coreferences[coref_subject] = []
                            dict_exact_matching_coreferences[coref_subject].append(span_counter)

                # find the main coreference chain
                if not dict_matching_coreferences:
                    sys.stdout.write('\tNo valid coreference chains\t')
                    enc_file_name = re.sub(r'\.txt', '', enc_file_name)
                    enc_file_name = re.sub(r' ', '_', enc_file_name)
                    most_instance_co = ''
                    next
                else:
                    most_instance_co = max(dict_matching_coreferences.iteritems(), key=operator.itemgetter(1))[0]


                coref_chains_to_use=set()
                coref_chains_to_use.add(most_instance_co)
                if use_more_chains==True:
                    for c in dict_exact_matching_coreferences:
                        coref_chains_to_use.add(c)
                for co in coref_chains_to_use:
                    sys.stdout.write(co)
                    sys.stdout.write('\t')
                # coref_chains_to_use.remove(most_instance_co)

                for token_obj in my_parser.get_tokens():
                    if not dict_sent.has_key(token_obj.get_sent()):
                        dict_sent[token_obj.get_sent()] = []
                    dict_sent[token_obj.get_sent()].append(token_obj.get_id())

                # find the term id of the coreferences
                for coref_obj in my_parser.get_corefs():
                    if coref_obj.get_id() in coref_chains_to_use:
                        for coref_span in coref_obj.get_spans():
                            for coref_span_term_id in coref_span.get_span_ids():
                                coref_span_term = my_parser.get_term(coref_span_term_id)
                                coref_span_term_w = coref_span_term.get_span().get_span_ids()[0]
                                list_coref_w.append(coref_span_term_w)


                sent_to_keep = set()
                for key in list_coref_w:
                    for sent_num in dict_sent.keys():
                        if key in dict_sent.get(sent_num):
                            sent_to_keep.add(sent_num)  # save the id of the sentences containing a coreference

                for sent_num in dict_sent.keys():
                    if sent_num not in sent_to_keep:
                        dict_sent.pop(sent_num)  # remove the sentences not containing coreferences

                # find word ids of temporal expressions
                for time_obj in my_parser.get_timeExpressions():
                    time_span = time_obj.get_span()
                    if time_span is not None:
                        if time_obj.get_type() == "DATE":
                            if not nottoinclude.search(time_obj.get_value()):
                                if time_obj.get_value()[:4].isdigit() is True:
                                    if int(time_obj.get_value()[:4]) <  1956 \
                                        and int(time_obj.get_value()[:4]) >  1899:
                                        time_span_list = time_span.get_span_ids()
                                        dict_timex[time_span_list[0]] = time_obj
                sent_to_keep = set()
                for key in dict_timex.keys():
                    for sent_num in dict_sent.keys():
                        if key in dict_sent.get(sent_num):
                            sent_to_keep.add(sent_num)  # save the id of the sentences containing a timex

                for sent_num in dict_sent.keys():
                    if sent_num not in sent_to_keep:
                        dict_sent.pop(sent_num)  # remove the sentences not containing a timex

                # find word ids of locations
                for entity_obj in my_parser.get_entities():
                    entity_type = entity_obj.get_type()
                    entity_id = entity_obj.get_id()
                    if (entity_type == 'LOCATION' or entity_type == 'ORGANIZATION'):
                        for entity_ref in entity_obj.get_references():
                            entity_head = \
                                (my_parser.get_term((entity_ref.get_span().get_span_ids())[0])).get_span().get_span_ids()[0]
                            dict_loc[entity_head] = entity_obj
                            for loc_span_id in entity_ref.get_span().get_span_ids():
                                for s in dict_t_to_w[loc_span_id]:
                                    dict_loc_full[s] = entity_obj

                sent_to_keep = set()
                for key in dict_loc.keys():
                    for sent_num in dict_sent.keys():
                        if key in dict_sent.get(sent_num):
                            sent_to_keep.add(
                                sent_num)  # save the id of the sentences containing a location

                for sent_num in dict_sent.keys():
                    if sent_num not in sent_to_keep:
                        dict_sent.pop(sent_num)  # remove the sentences not containing locations

                w_in_sentences = set()
                terms_in_sencences = set()

                # find the words in the candidate sentences (containing subject+location+temporal expression)
                for sentence_id in dict_sent.keys():
                    for w_id in dict_sent[sentence_id]:
                        w_in_sentences.add(w_id)
                    for w_t in w_in_sentences:
                        for w in dict_w_to_t[w_t]:
                            terms_in_sencences.add(w)

                    w_in_sentences = set()
                list_predicates_w = set()

                for my_clink in my_parser.get_predicates():
                    ext_refs = my_clink.get_external_references()
                    clink_span = my_clink.get_span()
                    timex_found = 0
                    location_found = 0
                    timex_to_print = ''
                    location_string = ''
                    need_theme = 0
                    need_cotheme = 0
                    theme_found = 0
                    cotheme_found = 0
                    need_entity = 0
                    entity_found = 0
                    need_resident = 0
                    resident_found = 0
                    need_self = 0
                    self_found = 0
                    need_student = 0
                    student_found = 0
                    need_employee = 0
                    employee_found = 0
                    negation_found = 0
                    need_nothing = None

                    for ref in ext_refs:

                        if "FrameNet" == ref.get_resource() and ("Fleeing" == ref.get_reference() \
                                    or "Motion" == ref.get_reference() \
                                    or "Cause_motion" == ref.get_reference() \
                                    or "Arriving" == ref.get_reference() \
                                    or "Travel" == ref.get_reference() \
                                    or "Attending" == ref.get_reference() \
                                    or "Meet_with" == ref.get_reference() \
                                    or "Scrutiny" == ref.get_reference() \
                                    or "Being_employed" == ref.get_reference() \
                                    or "Education_teaching" == ref.get_reference() \
                                    or "Come_together" == ref.get_reference() \
                                    or "Self_motion" == ref.get_reference() \
                                    or "Cotheme" == ref.get_reference() \
                                    or "Residence" == ref.get_reference() \
                                    or "State_continue" == ref.get_reference() \
                                    or "Departing" == ref.get_reference() \
                                    or "Colonization" == ref.get_reference() \
                                    or "Temporary_stay" == ref.get_reference() \
                                    or "Transfer" == ref.get_reference() \
                                    or "Transfer" == ref.get_reference() \
                                    or "Sending" == ref.get_reference()):

                                if "Sending" == ref.get_reference() \
                                        or "Departing" == ref.get_reference() \
                                        or "Transfer" == ref.get_reference():
                                    need_theme = 1
                                elif "Cotheme" == ref.get_reference():
                                    need_cotheme = 1
                                    need_theme = 1

                                elif "State_continue" == ref.get_reference():
                                    need_entity = 1
                                elif "Residence" == ref.get_reference():
                                    need_resident = 1
                                elif "Self_motion" == ref.get_reference():
                                    need_self = 1
                                elif "Education_teaching" == ref.get_reference():
                                    need_student = 1
                                elif "Being_employed" == ref.get_reference():
                                    need_employee = 1
                                else:
                                    need_nothing = True
                                resource = ref.get_resource()
                                reference = ref.get_reference()
                                roles = my_clink.get_roles()

                                # check if the frame is in the candidate sentences
                                for span in clink_span:
                                    if span.get_id() in terms_in_sencences:
                                        # find the id of the sentence for this frame
                                        for w in dict_t_to_w[span.get_id()]:
                                            for sent in dic_word_to_sentence[w]:
                                                id_sentence_to_print = sent
                                        timex_in_sentence = 1

                                        if timex_in_sentence == 1:
                                            # find the id of the sentence for the predicate
                                            for w in dict_t_to_w[span.get_id()]:
                                                for i_p_s in dic_word_to_sentence[w]:
                                                    id_precicate_sentence = i_p_s
                                                    for word in dic_sentence_to_words[id_precicate_sentence]:
                                                        if word in dict_timex:
                                                            timex = dict_timex[word]
                                                            t_timex_temp = dict_w_to_t[word]
                                                            for t_t in t_timex_temp:
                                                                t_timex=t_t
                                                            t_span_value = timex.get_value()
                                                            t_span_value = re.sub(r' ', '', t_span_value)
                                                            timex_found = 1
                                                            timex_to_print = timex_to_print + t_span_value + '_' + t_timex
                                                            timex_to_print = timex_to_print + ' '


                                        theme,employee,entity,resident,self_mover,student,cotheme = None, None, None, None, None, None, None

                                        for rol in roles:
                                            rol_span = rol.get_span()

                                            # check for negations
                                            if "AM-NEG" == rol.get_sem_role():
                                                negation_found = 1

                                            if need_cotheme == 1 and not cotheme:
                                                    if (role_filter('FrameNet' , 'Cotheme@Theme', rol , rol_span , dict_t_to_w, list_coref_w )) == 1:
                                                        theme = True

                                            if need_theme == 1 and not theme:
                                                    if (role_filter('FrameNet' , 'Theme', rol , rol_span , dict_t_to_w, list_coref_w )) == 1:
                                                        theme = True

                                            if need_entity == 1 and not entity:
                                                if (role_filter('FrameNet', 'Entity', rol, rol_span, dict_t_to_w, list_coref_w)) == 1:
                                                    entity = True

                                            if need_employee == 1 and not employee:
                                                if (role_filter('FrameNet', 'Employee', rol, rol_span, dict_t_to_w, list_coref_w)) == 1:
                                                    employee = True

                                            if need_resident == 1 and not resident:
                                                if (role_filter('FrameNet', 'Resident', rol, rol_span, dict_t_to_w, list_coref_w)) == 1:
                                                    resident = True

                                            if need_self == 1 and not self_mover:
                                                if (role_filter('FrameNet', 'Self_mover', rol, rol_span, dict_t_to_w, list_coref_w)) == 1:
                                                    self_mover = True

                                            if need_student == 1 and not student:
                                                if (role_filter('FrameNet', 'Student', rol, rol_span, dict_t_to_w, list_coref_w)) == 1:
                                                    student = True

                                            # find dates in the frame
                                            if timex_in_sentence == 0:
                                                rol_ext_ref = rol.get_external_references()
                                                for pred_ext_ref in rol_ext_ref:
                                                    if 'FrameNet' in pred_ext_ref.get_resource():
                                                        semantic_role = pred_ext_ref.get_reference()
                                                        sem_rol_split = semantic_role.split("@")
                                                        if sem_rol_split[1] == "Time":
                                                            for rol_id in rol_span:
                                                                for i in dict_t_to_w[rol_id.get_id()]:
                                                                    if i in dict_timex:
                                                                        timex = dict_timex[i]
                                                                        t_span_value = timex.get_value()
                                                                        timex_found = 1
                                                                        timex_to_print = timex_to_print + t_span_value
                                                                        timex_to_print = timex_to_print + ' '
                                                                        timex_role_to_print = '@' + sem_rol_split[1]


                                                    elif "TMP" in rol.get_sem_role():
                                                        for rol_id in rol_span:
                                                            for i in dict_t_to_w[rol_id.get_id()]:
                                                                if i in dict_timex:
                                                                    timex = dict_timex[i]
                                                                    t_span_value = timex.get_value()
                                                                    timex_found = 1
                                                                    timex_to_print = timex_to_print + t_span_value
                                                                    timex_to_print = timex_to_print + ' '
                                                                    timex_role_to_print = '@' + rol.get_sem_role()

                                            # filters to semantic roles for locations
                                            rol_ext_ref = rol.get_external_references()
                                            for pred_ext_ref in rol_ext_ref:
                                                if 'FrameNet' in pred_ext_ref.get_resource():
                                                    semantic_role = pred_ext_ref.get_reference()
                                                    sem_rol_split = semantic_role.split("@")
                                                    if sem_rol_split[1] == "Goal" \
                                                            or sem_rol_split[1] == "Place" \
                                                            or sem_rol_split[1] == "New_area" \
                                                            or sem_rol_split[1] == "Location" or ('Attending' == reference and sem_rol_split[1] == "Event"):
                                                        for rol_id in rol_span:
                                                            for i in dict_t_to_w[rol_id.get_id()]:
                                                                if i in dict_loc_full and location_found == 0:
                                                                    loc = dict_loc_full[i]
                                                                    for ref in loc.get_references():
                                                                        for loc_id in ref.get_span().get_span_ids():
                                                                            for w in dict_t_to_w[loc_id]:
                                                                                for w1 in dic_words[w]:
                                                                                    location_string = location_string + w1.encode("utf8")
                                                                                    location_string = location_string + ' '
                                                                                    location_found = 1
                                                        role_to_print = '@' + sem_rol_split[1]
                                                        break

                                                elif "AM-LOC" in rol.get_sem_role() \
                                                        or "AM-PMC" == rol.get_sem_role():
                                                    for rol_id in rol_span:
                                                        for i in dict_t_to_w[rol_id.get_id()]:
                                                            if i in dict_loc_full and location_found == 0:
                                                                loc = dict_loc_full[i]  # DIC_LOC
                                                                for ref in loc.get_references():
                                                                    for loc_id in ref.get_span().get_span_ids():
                                                                        for w in dict_t_to_w[loc_id]:
                                                                            for w1 in dic_words[w]:
                                                                                location_string = location_string + w1.encode("utf8")
                                                                                location_string = location_string + ' '
                                                                                location_found = 1
                                                    role_to_print = rol.get_sem_role()
                                                    break




                                                else:
                                                    if 'PropBank' in pred_ext_ref.get_resource():
                                                        semantic_role = pred_ext_ref.get_reference()
                                                        sem_rol_split = semantic_role.split("@")
                                                        if sem_rol_split[1] == "2"\
                                                            or sem_rol_split[1] == "4":
                                                            for rol_id in rol_span:
                                                                for i in dict_t_to_w[rol_id.get_id()]:
                                                                    if i in dict_loc_full and location_found == 0:
                                                                        loc = dict_loc_full[i]  # DIC_LOC
                                                                        for ref in loc.get_references():
                                                                            for loc_id in ref.get_span().get_span_ids():
                                                                                for w in dict_t_to_w[loc_id]:
                                                                                    for w1 in dic_words[w]:
                                                                                        location_string = location_string + w1.encode("utf8")
                                                                                        location_string = location_string + ' '
                                                                                        location_found = 1
                                                            role_to_print = '@' + sem_rol_split[1]
                                                            break


                                        sentence_to_print = ''
                                        for w in dic_sentence_to_words[id_sentence_to_print]:
                                            for w in dic_words[w]:
                                                sentence_to_print = sentence_to_print + w
                                                sentence_to_print = sentence_to_print + ' '

                                        timex_to_print = timex_to_print.strip(' ')
                                        location_string = location_string.strip(' ')
                                        sentence_to_print = sentence_to_print.strip(' ')

                                        exlude_predicate = 0
                                        # lemmas to discard
                                        if dict_t_to_lemma[span.get_id()] == "survey" \
                                                or dict_t_to_lemma[span.get_id()] == "investigation" \
                                                or dict_t_to_lemma[span.get_id()] == "investigate" \
                                                or dict_t_to_lemma[span.get_id()] == "run" \
                                                or dict_t_to_lemma[span.get_id()] == "walk" \
                                                or dict_t_to_lemma[span.get_id()] == "jump" \
                                                or dict_t_to_lemma[span.get_id()] == "file" \
                                                or dict_t_to_lemma[span.get_id()] == "swim" \
                                                or dict_t_to_lemma[span.get_id()] == "rush" \
                                                or dict_t_to_lemma[span.get_id()] == "occupy" \
                                                or dict_t_to_lemma[span.get_id()] == "approach" \
                                                or dict_t_to_lemma[span.get_id()] == "attract" \
                                                or dict_t_to_lemma[span.get_id()] == "force" \
                                                or dict_t_to_lemma[span.get_id()] == "press" \
                                                or dict_t_to_lemma[span.get_id()] == "throw" \
                                                or dict_t_to_lemma[span.get_id()] == "draw" \
                                                or dict_t_to_lemma[span.get_id()] == "learn" \
                                                or dict_t_to_lemma[span.get_id()] == "make" \
                                                or dict_t_to_lemma[span.get_id()] == "it" \
                                                or dict_t_to_lemma[span.get_id()] == "instruct" \
                                                or dict_t_to_lemma[span.get_id()] == "colony" \
                                                or dict_t_to_lemma[span.get_id()] == "settler" \
                                                or dict_t_to_lemma[span.get_id()] == "float" \
                                                or dict_t_to_lemma[span.get_id()] == "get" \
                                                or dict_t_to_lemma[span.get_id()] == "ship" \
                                                or dict_t_to_lemma[span.get_id()] == "stint" \
                                                or dict_t_to_lemma[span.get_id()] == "weave" \
                                                or dict_t_to_lemma[span.get_id()] == "inspector" \
                                                or dict_t_to_lemma[span.get_id()] == "Inspector" \
                                                or dict_t_to_lemma[span.get_id()] == "overnight" \
                                                or dict_t_to_lemma[span.get_id()] == "push":
                                            exlude_predicate = 1

                                        if negation_found == 0 and exlude_predicate == 0:
                                            if ((theme or employee or entity or resident or student or self_mover) or need_nothing) and (location_found == 1 and timex_found == 1):
                                                lat = ''
                                                lon = ''


                                                location_string = re.sub(r'[^A-Za-z](S|s)outh of ', '', location_string)
                                                location_string = re.sub(r'[^A-Za-z](N|n)orth of ', '', location_string)
                                                location_string = re.sub(r'[^A-Za-z](E|e)ast of ', '', location_string)
                                                location_string = re.sub(r'[^A-Za-z](W|w)est of ', '', location_string)
                                                location_string = re.sub(r'[^A-Za-z](S|s)outhern ', '', location_string)
                                                location_string = re.sub(r'[^A-Za-z](N|n)orthern ', '', location_string)
                                                location_string = re.sub(r'[^A-Za-z](E|e)astern ', '', location_string)
                                                location_string = re.sub(r'[^A-Za-z](W|w)eastern ', '', location_string)

                                                #### START GEOCODING
                                                (lat, lon) = georeference(location_string, nominatim_url)
                                                enc_file_name = re.sub(r'\.txt', '', enc_file_name)
                                                enc_file_name = re.sub(r' ', '_', enc_file_name)

                                                out_movements_file.write(enc_file_name)
                                                out_movements_file.write('\t')
                                                if use_pantheon_file==True:
                                                    out_movements_file.write(dict_nation[enc_file_name])
                                                    out_movements_file.write('\t')
                                                    out_movements_file.write(dict_continent[enc_file_name])
                                                    out_movements_file.write('\t')
                                                    out_movements_file.write(dict_gender[enc_file_name])
                                                    out_movements_file.write('\t')
                                                    out_movements_file.write(dict_profession[enc_file_name])
                                                    out_movements_file.write('\t')
                                                    out_movements_file.write(dict_group[enc_file_name])
                                                    out_movements_file.write('\t')
                                                    out_movements_file.write(dict_area[enc_file_name].strip('\n'))
                                                    out_movements_file.write('\t')
                                                else:
                                                    out_movements_file.write('null_\tnull_\tnull_\tnull_\tnull_\tnull_\t')
                                                out_movements_file.write(span.get_id())
                                                out_movements_file.write('\t')
                                                for predicate_w in dict_t_to_w[span.get_id()]:
                                                    for predicate_token in dic_words[predicate_w]:
                                                        out_movements_file.write(predicate_token)
                                                out_movements_file.write('\t')
                                                out_movements_file.write(resource)
                                                out_movements_file.write('\t')
                                                out_movements_file.write(reference)
                                                out_movements_file.write('\t')

                                                # clean duplicates
                                                timex_array_cell_to_remove = list()
                                                if ' ' in timex_to_print:
                                                    timex_no_interval = re.sub(r'\/[0-9]+', '', timex_to_print)
                                                    timex_list_splitted = timex_no_interval.split(" ")
                                                    distance = 999999999999999
                                                    predicate_senza_t = re.sub(r't', '', span.get_id())
                                                    for timex_item in timex_list_splitted:
                                                        timex_item_splitted = timex_item.split("_")
                                                        timex_senza_t = re.sub(r't', '', timex_item_splitted[1])
                                                        difference = int(predicate_senza_t) - int(timex_senza_t)
                                                        if abs(difference) < distance:
                                                            distance = abs(difference)
                                                            timex_to_print =timex_item_splitted[0]


                                                timex_to_print_list = timex_to_print.split("_")
                                                timex_to_print = timex_to_print_list[0]
                                                timex_to_print = re.sub(r'\*\*01', '0000', timex_to_print)
                                                timex_to_print = re.sub(r'\*\*02', '0000', timex_to_print)
                                                timex_to_print = timex_to_print.replace('-', '')
                                                out_movements_file.write(timex_to_print)
                                                out_movements_file.write('\t')
                                                out_movements_file.write(location_string)
                                                out_movements_file.write('\t')
                                                location_string = ''
                                                out_movements_file.write(role_to_print)
                                                role_to_print = ''
                                                out_movements_file.write('\t')
                                                out_movements_file.write('lat: ')
                                                out_movements_file.write(lat)
                                                out_movements_file.write('\t')
                                                out_movements_file.write('lon: ')
                                                out_movements_file.write(lon)
                                                out_movements_file.write('\t')
                                                out_movements_file.write (sentence_to_print.encode("utf8"))

                                                out_movements_file.write('\n')
                                                location_found = 0
                                                movements_number=movements_number+1
                                                movements_found = 1

                if movements_found == 0:
                    sys.stdout.write('No movements identified')
                else:
                    sys.stdout.write(str(movements_number)+' movements')
                    movements_number=0

                enc_file_name = re.sub(r'\.txt', '', enc_file_name)
                enc_file_name = re.sub(r' ', '_', enc_file_name)
                if use_pantheon_file == True:
                    out_movements_file.write( enc_file_name  + '\t' + dict_nation[enc_file_name] + '\t' + dict_continent[enc_file_name] + '\t' + dict_gender[enc_file_name] + '\t' + dict_profession[enc_file_name] + '\t' + dict_group[enc_file_name] + '\t' +dict_area[enc_file_name].strip('\n')  + '\t' + 'null_' + '\t' + 'null_' + '\t' + 'dbpedia_' + '\t' + 'Birth_' + '\t' + str(year_b) + '\t'+ location_b  + '\t' + 'null_' + "\t" + "lat: " + lat_b.encode("utf8") + '\t'+ "lon: " + lon_b.encode("utf8") )
                    out_movements_file.write('\tnull_\n')
                    out_movements_file.write( enc_file_name + '\t' + dict_nation[enc_file_name] + '\t' + dict_continent[enc_file_name] + '\t' + dict_gender[enc_file_name] + '\t' + dict_profession[enc_file_name] + '\t' + dict_group[enc_file_name] + '\t' +dict_area[enc_file_name].strip('\n') + '\t' + 'null_' + '\t' + 'null_' + '\t' + 'dbpedia_' + '\t' + 'Death_' + '\t' + str(year_d)+ '\t' + location_d + '\t' + 'null_' + "\t" + "lat: " + lat_d.encode("utf8") + '\t' + "lon: " + lon_d.encode("utf8") )
                    out_movements_file.write('\tnull_\n')
                else:
                    out_movements_file.write(enc_file_name + '\tnull_\tnull_\tnull_\tnull_\tnull_\tnull_\t' + 'null_' + '\t' + 'null_' + '\t' + 'dbpedia_' + '\t' + 'Birth_' + '\t' + str(year_b) + '\t' + location_b + '\t' + 'null_' + "\t" + "lat: " + lat_b.encode("utf8") + '\t' + "lon: " + lon_b.encode("utf8"))
                    out_movements_file.write('\tnull_\n')
                    out_movements_file.write(enc_file_name + '\tnull_\tnull_\tnull_\tnull_\tnull_\tnull_\t' + 'null_' + '\t' + 'null_' + '\t' + 'dbpedia_' + '\t' + 'Death_' + '\t' + str(year_d) + '\t' + location_d + '\t' + 'null_' + "\t" + "lat: " + lat_d.encode("utf8") + '\t' + "lon: " + lon_d.encode("utf8"))
                    out_movements_file.write('\tnull_\n')

    out_movements_file.close()
    sys.stdout.write('\n')



wiki_bio_download(list_file, "output_html_files")
clean_wiki_pages(list_file, "output_html_files", "output_txt_files")
txt_to_naf(list_file, "output_txt_files", "output_naf_files")
extract_movements ("output_naf_files",list_file, movements_output_file+".tsv", use_pantheon_file,pantheon_data_file,use_more_chains)
clean_na(movements_output_file+".tsv", movements_output_file+"_clean.tsv")
tab_to_json(movements_output_file+"_clean.tsv", movements_output_file+".json")
