

from optparse import Values
import sqlite3
from pandas import read_csv
from sqlite3 import connect
from pandas import read_sql, DataFrame, merge, concat



class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = ''


    def getdbPath(self):
        return self.dbPath

    def setdbPath(self, new_path):
        self.dbPath= new_path
        return True



class RelationalDataProcessor(RelationalProcessor):
    def __init__(self):
        #maybe this is extra
        super().__init__()



    def uploadData(self, file_path): 
        from pandas import read_sql, DataFrame
        from sqlite3 import connect
        
        #First let's populate our database with empty tables that have appropriate columns, to avoid queries failing 
        #completely in case some file is not uploaded.
        
        df_publications_empty=DataFrame (columns=["publicationInternalId", "id", "title", "type", "publication_year", "issue", "volume", "chapter", "publication_venue",
                                                "venue_type", "publisher", "event"], dtype="string")
        
        global_df_json_empty = DataFrame(columns=["venues_intid", "venues doi", "issn", "pub id", "auth doi", 
                                                  "auth_id", "doi", "family_name", "given_name", "orcid"], dtype="string")
        
        auth_and_publications_empty= DataFrame(columns=["pub id", "auth doi", "auth_id", "doi", 
                                                        "family_name", "given_name", "orcid"], dtype="string")
        
        person_df_empty= DataFrame(columns=["auth_id", "doi", "family_name", "given_name", "orcid"], dtype="string")
        
        auth_obj_empty = DataFrame(columns=["auth_doi", "author"], dtype="string")
        
        references_empty = DataFrame(columns=["ref_id", "ref doi", "citation"], dtype="string")
        
        ref_obj_empty= DataFrame(columns=["ref_doi", "cites"],dtype="string")
        
        publishers_empty= DataFrame(columns= ["pub_id", "publisher id", "name"], dtype="string")
        
        venues_json_empty= DataFrame(columns= ["venues_intid", "venues doi", "issn"], dtype="string")
        
        
        with connect(self.dbPath) as con:

                    df_publications_empty.to_sql("Publications", con, if_exists="append", index=False)
                    global_df_json_empty.to_sql("Global_json_dataframe", con, if_exists="append", index=False)
                    auth_and_publications_empty.to_sql("Authors and publications", con, if_exists="append", index=False)
                    person_df_empty.to_sql("Person", con, if_exists="append", index=False)
                    auth_obj_empty.to_sql("Authors_Obj", con, if_exists="append", index=False)
                    references_empty.to_sql("References", con, if_exists="append", index=False)
                    ref_obj_empty.to_sql("References_Obj", con, if_exists="append", index=False)
                    publishers_empty.to_sql("Publishers", con, if_exists="append", index=False)
                    venues_json_empty.to_sql("Venues_json", con, if_exists="append", index=False)
                    con.commit()
                    
                    
        #Now, let's upload our data accordingly            
        
        if ".csv" in file_path:
            from csv import reader
            with open (file_path, "r", encoding= "utf-8") as d:
                from pandas import Series, DataFrame, read_csv
                df_publications = read_csv (file_path, keep_default_na=False,
                                        dtype={
                                            "id": "string",
                                            "title": "string",
                                            "type":"string",
                                            "publication_year":"int",
                                            "issue": "string",
                                            "volume": "string",
                                            "chapter":"string",
                                            "publication_venue": "string",
                                            "venue_type": "string",
                                            "publisher":"string",
                                            "event":"string"
                                        })
                df_publications

                #Insert a series of internal ids for each publication
                
                publication_dois= df_publications[["id"]]
                publications_internal_id = []
                for idx, row in publication_dois.iterrows():

                    publications_internal_id.append("publication-" + str(idx))
                df_publications.insert(0, "publicationInternalId", Series(publications_internal_id, dtype="string"))
                df_publications


                #publishers
                publishers_l= (df_publications['publisher'].tolist())
                publishers_l


                publishers_unique = []
                [publishers_unique.append(x) for x in publishers_l if x not in publishers_unique]

                publishers_unique
                pub_int_id=[]
                for item in publishers_unique:
                    pub_int_id.append("pub-" + str(publishers_unique.index(item)))
                pub_int_id

                from pandas import concat, merge
                unique_publishers= Series(publishers_unique, name ="Publishers")
                publisher_int_id = Series(pub_int_id, name= "publisher_internal_id")
                unique_publishers_with_id = concat([unique_publishers, publisher_int_id],axis=1)


                #Retrieve data for journal articles

                journal_articles = df_publications.query("type == 'journal-article'")

                #Retrieve data for book chapters
                book_chapters = df_publications.query("type == 'book-chapter'")
                #Retrieve data for proceedings papers
                proceedings_paper = df_publications.query("type == 'proceedings-paper'")
            

                #Retrieve data for journals,books and proceedings
                journals = df_publications.query("venue_type == 'journal'")
                books = df_publications.query("venue_type == 'book'")
                proceedings = df_publications.query("venue_type == 'proceedings'")

                venues_csv=df_publications[["id","publication_venue", "venue_type","publisher"]]
                venues_csv.columns = ['publication doi', 'title', 'type', 'publisher']
                venues_csv=venues_csv.drop_duplicates()


                

                from sqlite3 import connect
                with connect(self.dbPath) as con:


                    df_publications.to_sql("Publications", con, if_exists="append", index=False)
                    venues_csv.to_sql("Venues csv", con, if_exists="append", index=False) 
                    books.to_sql("Books", con, if_exists="append", index=False )
                    journals.to_sql("Journals",con, if_exists="append", index=False)
                    book_chapters.to_sql("BookChapters", con, if_exists="append", index=False)
                    journal_articles.to_sql("JournalArticles", con, if_exists="append", index=False)
                    proceedings_paper.to_sql("ProceedingsPaper", con, if_exists="append", index=False)
                    proceedings.to_sql("Proceedings", con, if_exists="append", index=False)
                    unique_publishers_with_id.to_sql("Publishers csv", con, if_exists="append", index=False)

                    con.commit()
                    #Dataframes have been added with values from the csv

            return True

        elif ".json" in file_path:
            from pandas import Series, DataFrame
            from json import load
            with open (file_path, "r", encoding= "utf-8") as f:
                other_data= load(f)

                #separate each key from the dictionary

                other_data_authors = other_data.get("authors")
                other_data_venues = other_data.get("venues_id")
                other_data_ref = other_data.get("references")
                other_data_pub = other_data.get("publishers")

                #initialize empty lists for author data to be stored
                auth_doi=[]
                auth_fam=[]
                auth_giv=[]
                auth_id=[]

                for key in other_data_authors:   #each key has three values/dictionaries, name, given name and orcid.
                    a=[]
                    b=[]
                    c=[]
                    auth_val = other_data_authors[key] #for each key of the dictionary key which is the doi, append it to appropriate list
                    auth_doi.append(key)
                    for dict in auth_val:
                        a.append(dict["family"])  #for each dictionary of the doi-key, append its values to appropriate lists
                        b.append(dict["given"])
                        c.append(dict["orcid"])
                    auth_fam.append(a)            #populate the lists with the values
                    auth_giv.append(b)
                    auth_id.append(c)
                auth_doi_s=Series(auth_doi)       #transform lists to Series
                auth_fam_s=Series(auth_fam)

                auth_giv_s=Series(auth_giv)

                auth_id_s=Series(auth_id)

                                                  #create dataframe from the Series
                authors_df=DataFrame({
                    "auth doi" : auth_doi_s,
                    "family name" : auth_fam_s,
                    "given name" : auth_giv_s,
                    "orcid" : auth_id_s
                })
                authors_df
            


                dois_auth=authors_df[["auth doi"]]   #create and append internal id for the authors
                doi_int_id=[]
                for idx,row in dois_auth.iterrows():
                    doi_int_id.append("pub-" + str(idx))

                authors_df.insert(0, "pub id", Series(doi_int_id, dtype="string"))

                pub_id_and_doi= authors_df[["pub id", "auth doi"]]
                pub_id_and_doi.drop_duplicates


                #Create a dataframe for unique persons, skipping duplicates by not iterating over values already stored.
                #This method removes lists

                person_fam=[]
                person_giv=[]
                person_orcid=[]
                corr_doi=[]

                person_internal_id=[]

                for names_list in auth_fam:
                    i=auth_fam.index(names_list)
                    for name in names_list:
                        person_fam.append(name)
                        corr_doi.append(auth_doi[i])
                for given_list in auth_giv:
                    for name in given_list:
                        person_giv.append(name)
                for id_list in auth_id:
                    for id in id_list:
                        person_orcid.append(id)
                idx=0
                prov_list=[]
                for orcid in person_orcid:
                    if orcid not in prov_list:
                        prov_list.append(orcid)
                        person_internal_id.append("person-" + str(idx))
                        idx+=1
                    else:
                        idxj=prov_list.index(orcid)
                        person_internal_id.append(person_internal_id[idxj])

                #Dataframe for unique persons

                person_df=DataFrame({
                    "auth_id":person_internal_id,
                    "doi":corr_doi,
                    "family_name" : person_fam,
                    "given_name" : person_giv,
                    "orcid" : person_orcid
                })

                person_df
                unique_doi=[]
                
                list_authors=[]
                prov_list=[]
                string_list=[]
                idx=0
                for doi in corr_doi:
                    if doi not in unique_doi:
                        unique_doi.append(doi) 
                        prov_list=[]
                        prov_list.append(person_internal_id[idx])
                        list_authors.append(prov_list)
                        idx+=1
                    else:
                        prov_list.append(person_internal_id[idx])
                        idx+=1
                for list in list_authors:
                    string_authors=", ".join(list)
                    string_list.append(string_authors)

                authors_list=DataFrame({ "auth_doi":unique_doi, "author":string_list})       

                #create a dataframe with each person and their publications
                from pandas import merge
                auth_pub_with_id=merge(pub_id_and_doi, person_df, left_on="auth doi", right_on="doi")
                auth_pub_with_id



                # """Venues dataframe.
                # Same strategies as before but values of the dois' keys are not other dictionaries but lists
                # """

                venues_doi=[]
                venues_issn=[]

                for key in other_data_venues:
                    venues_val = other_data_venues[key]
                    venues_doi.append(key)
                    venues_issn.append(venues_val)

                venues_doi_s=Series(venues_doi)
                venues_issn_s=Series(venues_issn)


                #Create dataframe with the values of the json file.
                venues_df=DataFrame({
                    "venues doi" : venues_doi_s,
                    "issn" : venues_issn_s,
                })
                venues_df

                from pandas import merge

                venues_and_authors = merge(person_df, venues_df, left_on="doi", right_on="venues doi")
                venues_and_authors

                venues_doi=[]
                venues_issn=[]

                for key in other_data_venues:  #for each key- doi in our venues dictionary
                    venues_val = other_data_venues[key]
                    venues_doi.append(key)     #append the key, aka the doi.
                    venues_issn.append(venues_val) #append its value, aka the issn number.

                unique_venues=[]
                list_dois=[]


                #Create lists for the dataframe which will contain unique venues
                for i in range(len(venues_issn)):
                    if venues_issn[i] not in unique_venues:
                        unique_venues.append(venues_issn[i])
                        dois = []
                        dois.append(venues_doi[i])
                        list_dois.append(dois)
                    else:
                        j = unique_venues.index(venues_issn[i])
                        list_dois[j].append(venues_doi[i])

                un_venues_doi_s=Series(list_dois)
                un_venues_issn_s=Series(unique_venues)
                unique_venues_df=DataFrame({
                    "venues doi" : un_venues_doi_s,
                    "issn" : un_venues_issn_s,
                })

                unique_venues_df


                #Create internal identifiers for venues

                venues_ids = unique_venues_df[["venues doi", "issn"]]
                venues_internal_id = []
                for idx, row in venues_ids.iterrows():
                    venues_internal_id.append("venues-" + str(idx))
                venues_ids.insert(0, "venues id", Series(venues_internal_id, dtype="string"))
                venues_ids



                #Remove duplicates
                venues_id_intid=venues_ids.filter(["venues id"])
                venues_id_intid=venues_id_intid.values.tolist()
                venues_id_doi=venues_ids.filter(["venues doi"])
                venues_id_doi=venues_id_doi.values.tolist()
                venues_id_issn=venues_ids.filter(["issn"])
                venues_id_issn=venues_id_issn.values.tolist()

                l_intid=[]
                l_doi=[]
                l_issn=[]
                for outlist in venues_id_doi:
                    i=venues_id_doi.index(outlist)
                    for item in outlist:
                        for doi in item:
                            l_doi.append(doi)
                            l_intid.append(venues_id_intid[i][0])
                            str_issn=", ".join(venues_id_issn[i][0])
                            l_issn.append(str_issn)
                s_intid=Series(l_intid)
                s_doi=Series(l_doi)
                s_issn=Series(l_issn)
                unique_venues_ids_df=DataFrame({"venues_intid":s_intid, "venues doi":s_doi, "issn":s_issn})
                unique_venues_ids_df.drop_duplicates(subset="issn")
                


                


                #References dataframe
                # Same as venues, values of the keys are lists
                # """

                ref_doi=[]
                ref_cit=[]

                for key in other_data_ref:
                    ref_list = other_data_ref[key]
                    if len(ref_list)>0:
                        for cit in ref_list:
                            ref_doi.append(key)
                            ref_cit.append(cit)
                    else:
                        ref_doi.append(key)
                        ref_cit.append(ref_list)

                ref_doi_s=Series(ref_doi)
                ref_cit_s=Series(ref_cit)

                
                #Create the dataframe for references
                new_ref_df=DataFrame({
                    "ref doi" : ref_doi_s,
                    "citation" : ref_cit_s, 
                })
                
                ref_df=new_ref_df.astype({"citation": str}, errors='raise')
                
                ref_ids = ref_df[["ref doi", "citation"]]
                ref_internal_id = []
                for idx, row in ref_ids.iterrows():
                    ref_internal_id.append("citation-" + str(idx))
                ref_ids.insert(0, "ref_id", Series(ref_internal_id, dtype="string"))
                
                unique_doi=[]
                list_ref=[]
                prov_list=[]
                string_list=[]
                idx=0
                for doi in ref_doi:
                    if doi not in unique_doi:
                        unique_doi.append(doi) 
                        prov_list=[]
                        prov_list.append(ref_internal_id[idx])
                        list_ref.append(prov_list)
                        idx+=1
                    else:
                        prov_list.append(ref_internal_id[idx])
                        idx+=1
                for list in list_ref:
                    string_authors=", ".join(list)
                    string_list.append(string_authors)

                ref_list=DataFrame({ "ref_doi":unique_doi, "cites":string_list})   
                ref_list
                
                # Publishers dataframe
                # The dois' keys' values are just dictionaries
                #create a list for each dictionary, crossref, id and name

                pub_cr=[]           
                pub_id=[]
                pub_name=[]

                for key in other_data_pub:
                    pub_val = other_data_pub[key]
                    pub_cr.append(key)
                    pub_id.append(pub_val["id"])
                    pub_name.append(pub_val["name"])

                pub_cr_s=Series(pub_cr)
                pub_id_s=Series(pub_id)
                pub_name_s=Series(pub_name)

                publishers_df=DataFrame({
                    "crossref" : pub_cr_s,
                    "publisher id" : pub_id_s,
                    "name" : pub_name_s,
                })
                publishers_df

                

                from pandas import merge


                publishers_ids = publishers_df[["publisher id", "name"]]

                # Generate a list of internal identifiers for the publishers
                pub_internal_id = []
                for idx, row in publishers_ids.iterrows():
                    pub_internal_id.append("publisher-" + str(idx))

                # Add the list of venues internal identifiers as a new column of the data frame via the class 'Series'
                publishers_ids.insert(0, "pub_id", Series(pub_internal_id, dtype="string"))

                # Show the new data frame on screen
                publishers_ids
                
                #Create a dataframe with all information retrieved from venues
                global_df= merge(unique_venues_ids_df, auth_pub_with_id, left_on="venues doi", right_on="doi")
                
                from sqlite3 import connect

                with connect(self.dbPath) as con:

                    unique_venues_ids_df.to_sql("Venues_json", con, if_exists="append", index=False)
                    person_df.to_sql("Person", con, if_exists ="append", index=False)
                    authors_list.to_sql("Authors_Obj", con, if_exists ="append", index=False)
                    global_df.to_sql("Global_json_dataframe", con, if_exists="append", index= False)
                    auth_pub_with_id.to_sql("Authors and publications", con, if_exists="append", index=False)
                    ref_ids.to_sql("References", con, if_exists="append", index=False)
                    ref_list.to_sql("References_Obj", con, if_exists="append", index=False)
                    publishers_ids.to_sql("Publishers", con, if_exists = "append", index=False)
                    con.commit()

                return True


        else: return False +"Please upload a file with format '.json' or '.csv"

class RelationalQueryProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()



    def getPublicationsPublishedInYear(self,year):
        with connect(self.dbPath) as con:
            query= """SELECT DISTINCT "id", "title", "publication_year", "publication_venue", "author", "cites"
            FROM "Publications"
            LEFT JOIN Authors_Obj
            ON Publications.id = Authors_Obj.auth_doi
            LEFT JOIN References_Obj
            ON Publications.id = References_Obj.ref_doi
            WHERE publication_year =?"""
            dframe =read_sql(query,con,params=[year])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue"})
            return dframe


    def getPublicationsByAuthorId(self, id):
        with connect(self.dbPath) as con:
          
            query="""SELECT DISTINCT "id", "title", "publication_year", "publication_venue", "author", "ref_id" 
            FROM "Authors and Publications" 
            LEFT JOIN "Publications" 
            ON "Authors and Publications".doi = Publications.id 
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            LEFT JOIN Authors_Obj
            ON Publications.id = Authors_Obj.auth_doi
            WHERE orcid =?"""
            dframe=read_sql(query,con, params=[id])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue", "ref_id":"cites"})
            dframe.astype({"publicationYear":"int"})
            return dframe


    def getMostCitedPublication(self): 
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "id", "title", "publication_year", "publication_venue", "author", "ref_id",   
                    COUNT("citation") AS value_occurrence 
                    FROM "References" LEFT JOIN "Publications" 
                    ON "References".'citation' = "Publications".id
                    LEFT JOIN Authors_Obj
                    ON Publications.id = Authors_Obj.auth_doi
                    WHERE "citation" != "[]"
                    GROUP BY "citation"
                    ORDER BY value_occurrence DESC 
                    """
            dframe =read_sql(query,con)
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue", "'auth_id'":"author", "ref_id":"cites"})
            dframe.astype({"publicationYear":"int"})
            col_ind=6
            for idx, row in dframe.iterrows():
                if dframe.iloc[idx+1, col_ind]<dframe.iloc[idx, col_ind]:
                    return dframe.iloc[:idx+1]
                                
                    
    def getMostCitedVenue(self): 
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "issn", "publication_venue", "publisher",  
                    COUNT("publication_venue") AS value_occurrence 
                    FROM "References" 
                    LEFT JOIN "Publications" 
                    ON "References".'citation' = "Publications".id
                    LEFT JOIN "Venues_json"
                    ON "Publications".id="Venues_json".'venues doi'
                    GROUP BY "publication_venue"
                    ORDER BY value_occurrence DESC 
                    """
            dframe =read_sql(query,con)
            dframe=dframe.rename(columns={"publication_venue":"title", "issn":"id"})
            col_ind=3
            for idx, row in dframe.iterrows():
                if dframe.iloc[idx+1, col_ind]<dframe.iloc[idx, col_ind]:
                    return dframe.iloc[:idx+1]


    def getPublicationInVenue(self, venue_id):
        with connect(self.dbPath) as con: 
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "publication_year", "publication_venue", "auth_id", "ref_id"
            FROM Publications 
            LEFT JOIN Global_json_dataframe 
            ON Publications.id = Global_json_dataframe.doi
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            WHERE issn LIKE ?"""
            dframe= read_sql(query,con, params=['%'+venue_id+'%'])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe

    def getVenuesByPublisherId(self, publisher):
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "publication_venue", "publisher", "issn" 
            FROM Publications
            LEFT JOIN Venues_json
            ON Publications.id=Venues_json.'venues doi'
            WHERE publisher=?"""
            dframe= read_sql(query,con,params=[publisher])
            dframe=dframe.rename(columns={"publication_venue":"title", "issn":"id"})
            return dframe

    def getJournalArticlesInVolume(self, volume, venue_id):
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "issue", "volume",  
            "publication_year", "publication_venue", "auth_id", "ref_id"  FROM Publications 
            LEFT JOIN 
            Global_json_dataframe ON Publications.id = Global_json_dataframe.doi
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            WHERE issn LIKE ? AND volume=?"""
            dframe= read_sql(query,con,params=['%'+venue_id+'%',volume])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe

    def getJournalArticlesInIssue(self, issue, volume, venue_id):
         with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "issue", "volume",  "publication_year",
            "publication_venue", "auth_id","ref_id"  FROM "Publications" 
            LEFT JOIN 
            Global_json_dataframe ON Publications.id = Global_json_dataframe.doi
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            WHERE issn LIKE ? AND volume=? AND issue=? AND type=='journal-article'"""
            dframe= read_sql(query,con,params=['%'+venue_id+'%', volume, issue])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe 

    def getJournalArticlesInJournal(self, venue_id):
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "issue", "volume",  
            "publication_year", "publication_venue", "auth_id", "ref_id"  FROM Publications 
            LEFT JOIN Global_json_dataframe ON Publications.id = Global_json_dataframe.doi 
            WHERE issn LIKE ? AND type=='journal-article'"""
            dframe=read_sql(query,con,params=['%'+venue_id+'%'])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe  
    
    def getProceedingsByEvent(self, event):
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "publication_year", "publication_venue", 
            'auth_id', "event", 'ref_id' FROM Publications WHERE event LIKE ?"""
            dframe=read_sql(query, con, params=['%'+event+'%'])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue", "'auth_id'":"author", "'ref_id'":"cites"})            
            return dframe
    

    def getPublicationAuthors(self, doi):
        with connect(self.dbPath) as con:
            query="SELECT DISTINCT auth_id, family_name, given_name, orcid FROM Person WHERE doi =?"
            dframe=read_sql(query,con,params=[doi])
            dframe=dframe.rename(columns={"orcid":"id", "family_name":"familyName", "given_name":"givenName"})
            return dframe

    def getPublicationsByAuthorName(self, auth_name): #here it is assumed that both given name and family name can be given as input
        with connect(self.dbPath) as con:
            auth_name=auth_name.capitalize()
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "publication_year", "publication_venue", "auth_id", "ref_id" 
            FROM "Authors and Publications" 
            LEFT JOIN "Publications" 
            ON "Authors and Publications".doi = Publications.id
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            WHERE family_name LIKE ? OR given_name LIKE ?"""
            dframe= read_sql(query,con,params=['%'+auth_name+'%','%'+auth_name+'%'])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe

    def getDistinctPublisherOfPublications(self, doi):
        result=[]
        with connect(self.dbPath) as con:
            for i in doi:
            
                cur= con.cursor()
                query="""SELECT DISTINCT name, publisher from Publishers LEFT JOIN Publications 
                ON Publishers.'publisher id' == Publications.'publisher' WHERE id=?"""
                cur.execute(query,(i,))
                result.append(cur.fetchone())
            dframe= DataFrame(result,columns=["name","id"])
            return dframe
    


# fede_test= RelationalDataProcessor()
# fede_test.setdbPath("test_definitivo.db")
# fede_test.uploadData("relational_publications.csv")
# fede_test.uploadData("relational_other_data.json")
# #print(fede_test.getdbPath())
# rlp_query = RelationalQueryProcessor()
# rlp_query.setdbPath("test_definitivo.db")

#print(rlp_query.getPublicationsPublishedInYear(2020))
#rlp_query.getPublicationsByAuthorId("0000-0001-5366-5194")
#print(rlp_query.getVenuesByPublisherId("crossref:78"))
#print(rlp_query.getPublicationInVenue("issn:0944-1344"))
# print(rlp_query.getJournalArticlesInIssue("1", "12", "issn:1758-2946"))
#rlp_query.getDistinctPublisherOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"])
#rlp_query.getMostCitedPublication()



from pandas import read_csv
from json import load
from pandas import Series
from pandas import DataFrame
from pandas import merge
from rdflib import Graph
from rdflib import URIRef
from rdflib import Literal
from rdflib import RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

class TriplestoreProcessor(object): 
    def __init__(self): 
        self.endpointUrl=''

    def getEndpointUrl(self):
        return self.endpointUrl
    
    def setEndpointUrl(self, url):
        self.endpointUrl = url
        return True

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self) -> None:
        super().__init__()

    def uploadData(self, path):

        #find the endpoint:
        endpoint = self.getEndpointUrl()

        #is there any statement stored in my endpoint?
        store = SPARQLUpdateStore()
        store.open((endpoint, endpoint))

        triples_number = store.__len__(context=None)
            
        # Once finished, remeber to close the connection
        store.close()

        #create a blank graph
        my_graph = Graph()

        # some classes of resources (not finished yet)
        JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
        BookChapter = URIRef("https://schema.org/Chapter")
        ProceedingsP = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")

        Journal = URIRef("https://schema.org/Periodical")
        Book = URIRef("https://schema.org/Book")
        Proceeding = ("https://schema.org/Event")

        # some attributes related to classes (not finished yet)
        doi = URIRef("https://schema.org/identifier")
        publicationYear = URIRef("https://schema.org/datePublished")
        title = URIRef("https://schema.org/name")
        issue = URIRef("https://schema.org/issueNumber")
        volume = URIRef("https://schema.org/volumeNumber")
        chapter_num = URIRef("https://schema.org/numberedPosition")
        publisher = URIRef("https://schema.org/publisher")
        publicationVenue = URIRef("https://schema.org/isPartOf")
        author = URIRef("https://schema.org/author")
        name = URIRef("https://schema.org/givenName")
        surname = URIRef("https://schema.org/familyName")
        citation = URIRef("https://schema.org/citation")
        event = URIRef("https://schema.org/recordedIn")

        #base url for our subjects
        base_url = "https://in.io/res/"

        #CSV file loading:
        if ".csv" in path:
            count = 0
            publ_venues_dict = {}
            publisher_dict = {}
            venues_idx = 0
            publications = read_csv (path, keep_default_na=False,
                               dtype={
                                   "id": "string",
                                   "title": "string",
                                   "type":"string",
                                   "publication_year":"int",
                                   "issue": "string",
                                   "volume": "string",
                                   "chapter":"string",
                                   "publication_venue": "string",
                                   "venue_type": "string",
                                   "publisher":"string",
                                   "event":"string"
                               })
            if triples_number == 0:
                for idx, row in publications.iterrows():
                    local_id = "publication-" + str(idx)

                    
                    # The shape of the new resources that are publications is
                    # 'https://comp-data.github.io/res/publication-<integer>'
                    subj = URIRef(base_url + local_id)

                    #In this first case, the if condition tells us if each of the publications is of type "journal-article"
                    #(look at the dataframe above)
                    if row["type"] == "journal-article":
                        my_graph.add((subj, RDF.type, JournalArticle))
                    
                        # These two statements applies only to journal article:
                        my_graph.add((subj, issue, Literal(row["issue"]))) 
                        my_graph.add((subj, volume, Literal(row["volume"]))) 
                    
                    elif row["type"] == "book-chapter":
                        my_graph.add((subj, RDF.type, BookChapter))
                        my_graph.add((subj, chapter_num, Literal(row["chapter"])))
                    
                    elif row["type"] == "proceedings-paper":
                        my_graph.add((subj, RDF.type, ProceedingsP))
                    
                    
                    my_graph.add((subj, title, Literal(row["title"])))
                    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    my_graph.add((subj, doi, Literal(row["id"])))
                    
                    #how to deal with publication_venues? We check whether they are included or not in the graph: then, we link it to the publication 
                    if row["publication_venue"] not in publ_venues_dict:   
                        venues_id = "venue-" + str(len(publ_venues_dict))
                        venue_subj = URIRef(base_url + venues_id)
                        publ_venues_dict[row["publication_venue"]] = venue_subj
                        my_graph.add((subj, publicationVenue, venue_subj))
                        
                        if row["venue_type"] == "journal": 
                            my_graph.add((venue_subj, RDF.type, Journal))
                        elif row["venue_type"] == "book":
                            my_graph.add((venue_subj, RDF.type, Book))
                        elif row["venue_type"] == "proceedings":
                            my_graph.add((venue_subj, RDF.type, Proceeding))
                            my_graph.add((venue_subj, event, Literal(row["event"])))
                        my_graph.add((venue_subj, title, Literal(row["publication_venue"])))

                        
                    elif row["publication_venue"] in publ_venues_dict:
                        venue_subj = publ_venues_dict[row["publication_venue"]]
                        my_graph.add((subj, publicationVenue, venue_subj))

                    if row["publisher"] not in publisher_dict:
                        publisher_subj = URIRef(base_url + "publisher-" + str(len(publisher_dict)))
                        publisher_dict[row["publisher"]] = publisher_subj
                        my_graph.add((venue_subj, publisher, publisher_subj))
                        my_graph.add((publisher_subj, doi, Literal(row["publisher"])))
                    else:
                        publisher_subj = publisher_dict[row["publisher"]]
                        my_graph.add((venue_subj, publisher, publisher_subj))

            elif triples_number > 0:
                #I check how many publications have already been introduced in the graph (each of them with a different URI)
                #the regex "doi" allows me to avoid misunderstanding with other types of identifier (for authors, venues, etc.)
                new_query = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publication
                WHERE {
                    ?publication schema:identifier ?identifier .
                    FILTER regex(?identifier, "doi")
                }
                """
                result_df = get(endpoint, new_query, True)
                number_of_publications = result_df.shape[0]

                how_many_venues ="""
                PREFIX schema: <https://schema.org/>
                SELECT DISTINCT ?venue
                WHERE {
                ?publ schema:isPartOf ?venue .
                }
                """
                venues_idx = get(endpoint, how_many_venues, True).shape[0]
                for idx, row in publications.iterrows():
                    df_doi = row["id"]
                    #is the doi in the store?
                    query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    check_doi = get(endpoint, query.format(df_doi), True)
                    if check_doi.empty:
                        #it means that no-info about that publication is available on the database
                        subj = URIRef(base_url + "publication-" + str(number_of_publications + count))
                        count +=1
                        my_graph.add((subj, doi, Literal(df_doi)))

                        #check whether the publication_venue has already been defined in the graph-database:
                        #I'm still inside the check_doi.empty condition, so there is no possibility that some information about that venue  
                        #has been considered, unless the venue also contains another publication which I have already presented in the database.
                        #In other words, different publications from different CSV files, but in both the CSV files they will have the same title.
                        venue_query ="""
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            ?publication schema:isPartOf ?venue .
                            ?venue schema:name "{0}" .
                        }}
                        """
                        publications_inside_a_venue = get(endpoint, venue_query.format(row["publication_venue"]), True)
                        if publications_inside_a_venue.empty:
                            if row["publication_venue"] in publ_venues_dict:
                                venue_subj = publ_venues_dict[row["publication_venue"]]
                            else:
                            #I create a new URIRef for that specific venue
                                venue_subj = URIRef(base_url + "venue-" + str(venues_idx + len(publ_venues_dict)))
                                publ_venues_dict[row["publication_venue"]] = venue_subj
                                if row["venue_type"] == "journal": 
                                    my_graph.add((venue_subj, RDF.type, Journal))
                                elif row["venue_type"] == "book":
                                    my_graph.add((venue_subj, RDF.type, Book))
                                elif row["venue_type"] == "proceedings":
                                    my_graph.add((venue_subj, RDF.type, Proceeding))
                                    my_graph.add((venue_subj, event, Literal(row["event"])))
                                my_graph.add((venue_subj, title, Literal(row["publication_venue"])))
                        else:
                            venue_subj = URIRef(publications_inside_a_venue.at[0, "venue"])
                            if row["venue_type"] == "journal": 
                                    my_graph.add((venue_subj, RDF.type, Journal))
                            elif row["venue_type"] == "book":
                                my_graph.add((venue_subj, RDF.type, Book))
                            my_graph.add((venue_subj, title, Literal(row["publication_venue"])))
                        my_graph.add((subj, publicationVenue, venue_subj))
                    else:
                        #the doi is already expressed in the database: what is the URIRef of the specific publication?
                        new_query = """
                        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX schema: <https://schema.org/>

                        SELECT ?publication
                        WHERE {{
                            ?publication schema:identifier "{0}"
                        }}
                        """
                        subj = URIRef(get(endpoint, new_query.format(df_doi), True).at[0, 'publication'])

                        #is the doi already linked to a venue in my database?
                        venue_query ="""
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            ?publication schema:isPartOf ?venue .
                            ?publication schema:identifier "{0}" .
                        }}
                        """
                        venues_and_doi = get(endpoint, venue_query.format(df_doi), True)
                        if venues_and_doi.empty:
                            #the publication is not associated with a venue
                            if row["publication_venue"] in publ_venues_dict:
                                venue_subj = publ_venues_dict[row["publication_venue"]]
                            else:
                                venue_query ="""
                                PREFIX schema: <https://schema.org/>
                                SELECT ?venue
                                WHERE {{
                                    ?publication schema:isPartOf ?venue .
                                    ?venue schema:name "{0}" .
                                }}
                                """
                                check_venue = get(endpoint, venue_query.format(row["publication_venue"]), True)
                                if check_venue.empty:
                                    #I create a new URIRef for that specific venue
                                    venue_subj = URIRef(base_url + "venue-" + str(venues_idx + len(publ_venues_dict)))
                                    publ_venues_dict[row["publication_venue"]] = venue_subj
                                    if row["venue_type"] == "journal": 
                                        my_graph.add((venue_subj, RDF.type, Journal))
                                    elif row["venue_type"] == "book":
                                        my_graph.add((venue_subj, RDF.type, Book))
                                    my_graph.add((venue_subj, title, Literal(row["publication_venue"])))
                                else:
                                    venue_subj = URIRef(check_venue.at[0, 'venue'])
                                    
                        else: 
                            venue_subj = URIRef(venues_and_doi.at[0, 'venue'])

                        if row["venue_type"] == "journal": 
                            my_graph.add((venue_subj, RDF.type, Journal))
                        elif row["venue_type"] == "book":
                            my_graph.add((venue_subj, RDF.type, Book))
                        my_graph.add((venue_subj, title, Literal(row["publication_venue"])))
                        my_graph.add((subj, publicationVenue, venue_subj))
                        
                    if row["type"] == "journal-article":
                        my_graph.add((subj, RDF.type, JournalArticle))
                    
                        # These two statements applies only to journal article:
                        my_graph.add((subj, issue, Literal(row["issue"]))) 
                        my_graph.add((subj, volume, Literal(row["volume"]))) 
                    
                    elif row["type"] == "book-chapter":
                        my_graph.add((subj, RDF.type, BookChapter))
                        my_graph.add((subj, chapter_num, Literal(row["chapter"])))
                    
                    elif row["type"] == "proceedings-paper":
                        my_graph.add((subj, RDF.type, ProceedingsP))
                    my_graph.add((subj, title, Literal(row["title"])))
                    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    
                                        
                    publisher_check="""
                    PREFIX schema:<https://schema.org/>
                    SELECT ?publisher
                    WHERE {{
                        ?publisher schema:identifier "{0}"
                    }}
                    """
                    publisher_check_df = get(endpoint, publisher_check.format(row["publisher"]), True)
                    if publisher_check_df.empty:
                        if row["publisher"] in publisher_dict:
                            publisher_subj = publisher_dict[row["publisher"]]
                            my_graph.add((venue_subj, publisher, publisher_subj))
                        else:
                            how_many_publishers = """
                            PREFIX schema:<https://schema.org/>
                            SELECT ?publisher
                            WHERE {
                                ?publisher schema:identifier ?pub_id .
                                FILTER regex(?pub_id, "crossref")
                            }
                            """
                            how_many_publishers_idx = get(endpoint, how_many_publishers, True).shape[0]
                            publisher_subj = URIRef(base_url + "publisher-" + str(len(publisher_dict) + how_many_publishers_idx))
                            my_graph.add((venue_subj, publisher, publisher_subj))
                            my_graph.add((publisher_subj, doi, Literal(row["publisher"])))
                            publisher_dict[row["publisher"]] = publisher_subj
                    else:
                        publisher_subj = URIRef(publisher_check_df.at[0, "publisher"])
                        my_graph.add((venue_subj, publisher, publisher_subj))




        #JSON file loading:
        elif ".json" in path:
            with open(path, "r", encoding="utf-8") as f:
                graph_other_data = load(f)

            graph_other_data_authors = graph_other_data.get("authors")
            graph_other_data_venues = graph_other_data.get("venues_id")
            graph_other_data_ref = graph_other_data.get("references")
            graph_other_data_pub = graph_other_data.get("publishers")

            #authors
            auth_doi=[]
            auth_fam=[]
            auth_giv=[]
            auth_id=[]

            for key in graph_other_data_authors:
                a=[]
                b=[]
                c=[]
                auth_val = graph_other_data_authors[key]
                auth_doi.append(key)
                for dict in auth_val:
                    a.append(dict["family"])
                    b.append(dict["given"])
                    c.append(dict["orcid"])
                auth_fam.append(a)
                auth_giv.append(b)
                auth_id.append(c)

                auth_doi_s=Series(auth_doi)
            auth_fam_s=Series(auth_fam)
            auth_giv_s=Series(auth_giv)
            auth_id_s=Series(auth_id)

            authors_df=DataFrame({
                "auth doi" : auth_doi_s,
                "family name" : auth_fam_s, 
                "given name" : auth_giv_s, 
                "orcid" : auth_id_s
            })

            #venues
            venues_doi=[]
            venues_issn=[]

            for key in graph_other_data_venues:
                venues_val = graph_other_data_venues[key]
                venues_doi.append(key)
                venues_issn.append(venues_val)

            unique_venues=[]
            list_dois=[]

            for i in range(len(venues_issn)):
                if venues_issn[i] not in unique_venues:
                    unique_venues.append(venues_issn[i])
                    dois = []
                    dois.append(venues_doi[i])
                    list_dois.append(dois)
                else:
                    j = unique_venues.index(venues_issn[i])
                    list_dois[j].append(venues_doi[i])

            un_venues_doi_s=Series(list_dois)
            un_venues_issn_s=Series(unique_venues)        
            unique_venues_df=DataFrame({
                "venues doi" : un_venues_doi_s,
                "issn" : un_venues_issn_s, 
            })

            venues_ids = unique_venues_df[["venues doi", "issn"]]
            venues_internal_id = []
            for idx, row in venues_ids.iterrows():
                venues_internal_id.append("venues-" + str(idx))
            venues_ids.insert(0, "venues id", Series(venues_internal_id, dtype="string"))
            venues_id_intid=venues_ids.filter(["venues id"])
            venues_id_intid=venues_id_intid.values.tolist()
            venues_id_doi=venues_ids.filter(["venues doi"])
            venues_id_doi=venues_id_doi.values.tolist()
            venues_id_issn=venues_ids.filter(["issn"])
            venues_id_issn=venues_id_issn.values.tolist()

            l_intid=[]
            l_doi=[]
            l_issn=[]
            for outlist in venues_id_doi:
                i=venues_id_doi.index(outlist)
                for list in outlist:
                    for dois in list:
                        l_doi.append(dois)
                        l_intid.append(venues_id_intid[i][0])
                        l_issn.append(venues_id_issn[i][0])
            s_intid=Series(l_intid)
            s_doi=Series(l_doi)
            s_issn=Series(l_issn)
            unique_venues_ids_df=DataFrame({"venues_intid":s_intid, "venues doi":s_doi, "issn":s_issn})

            #references
            ref_doi=[]
            ref_cit=[]

            for key in graph_other_data_ref:
                ref_val = graph_other_data_ref[key]
                ref_doi.append(key)
                ref_cit.append(ref_val)

            ref_doi_s=Series(ref_doi)
            ref_cit_s=Series(ref_cit)

            ref_df=DataFrame({
                "ref doi" : ref_doi_s,
                "citation" : ref_cit_s, 
            })

            #publishers
            pub_cr=[]
            pub_id=[]
            pub_name=[]

            for key in graph_other_data_pub:
                pub_val = graph_other_data_pub[key]
                pub_cr.append(key)
                pub_id.append(pub_val["id"])
                pub_name.append(pub_val["name"])

            pub_cr_s=Series(pub_cr)
            pub_id_s=Series(pub_id)
            pub_name_s=Series(pub_name)

            publishers_df=DataFrame({
                "crossref" : pub_cr_s,
                "publisher id" : pub_id_s, 
                "name" : pub_name_s, 
            })

            venues_authors = merge(unique_venues_ids_df, authors_df, left_on="venues doi", right_on="auth doi", how="outer")
            venues_authors_ref = merge(venues_authors, ref_df, left_on="venues doi", right_on="ref doi", how="outer")

            authors_dict = {}
            publications_possibly_reused = {}
            a_list = [] #I need this list to create a type comparison
            venues_dict = {}
            publisher_dict = {}

            if triples_number == 0:
                for idx, row in venues_authors_ref.iterrows():
                    local_id = "publication-" + str(idx)
                    #authors
                    if type(row["orcid"]) == type(a_list):
                        for i in range(len(row["orcid"])):
                            subj = URIRef(base_url + local_id)
                            publications_possibly_reused[row["auth doi"]] = subj
                            if row["orcid"][i] not in authors_dict:
                                author_subj = URIRef(base_url + "author-" + str(len(authors_dict)))
                                authors_dict[row["orcid"][i]] = author_subj
                                my_graph.add((author_subj, doi, Literal(row["orcid"][i])))
                                my_graph.add((author_subj, name, Literal(row["given name"][i])))
                                my_graph.add((author_subj, surname, Literal(row["family name"][i])))
                            else: 
                                author_subj = authors_dict[row["orcid"][i]]
                            my_graph.add((subj, doi, Literal(row["auth doi"])))
                            my_graph.add((subj, author, author_subj))

                    #venues
                    #check if the publication is already associated with a URIRef:
                    if row["venues doi"] in publications_possibly_reused:
                        a_subj = publications_possibly_reused[row["venues doi"]]
                    else: 
                        new_idx = len(publications_possibly_reused)
                        a_subj = URIRef(base_url + "publication-" + str(new_idx))
                        publications_possibly_reused[row["venues doi"]] = a_subj
                    if type(row["issn"]) == type(a_list) and len(row["issn"]) >= 1:
                        #is the publication associated with a venue URIRef?
                        list_to_string = ' '.join(row["issn"])
                        if list_to_string in venues_dict: 
                            venue_subj = venues_dict[list_to_string]
                        else:
                            venue_subj = URIRef(base_url + "venue-" + str(len(venues_dict))) 
                            venues_dict[list_to_string] = venue_subj
                            for j in range(len(row["issn"])):
                                my_graph.add((venue_subj, doi, Literal(row["issn"][j])))  
                        my_graph.add((a_subj, publicationVenue, venue_subj))
                                
                    
                    
                    #citations
                    if row["ref doi"] in publications_possibly_reused:
                        subj = publications_possibly_reused[row["ref doi"]]
                    else: 
                        new_idx = len(publications_possibly_reused)
                        subj = URIRef(base_url + "publication-" + str(new_idx))
                        my_graph.add((subj, doi, Literal(row["ref doi"])))
                        publications_possibly_reused[row["ref doi"]] = subj
                    if type(row["citation"]) == type(a_list):
                        for w in range(len(row["citation"])):
                            #have I already created a URIRef for the cited publication?
                            if row["citation"][w] in publications_possibly_reused:
                                cited_publ = publications_possibly_reused[row["citation"][w]]
                            else:
                                extra_idx = len(publications_possibly_reused)
                                cited_publ = URIRef(base_url + "publication-" + str(extra_idx))
                                publications_possibly_reused[row["citation"][w]] = cited_publ
                            my_graph.add((subj, citation, cited_publ))
                
                for idx, row in publishers_df.iterrows():
                    subj = URIRef(base_url + "publisher-" + str(idx))
                    my_graph.add((subj, doi, Literal(row["crossref"])))
                    my_graph.add((subj, title, Literal(row["name"])))        
                
            elif triples_number > 0:
                #how many authors have already been introduced in the db?
                authors_in_db = """
                PREFIX schema: <https://schema.org/>

                SELECT DISTINCT ?author
                WHERE {
                    ?publication schema:author ?author .
                }
                """
                authors_in_db_df = (get(endpoint, authors_in_db, True)).shape[0]
                venues_in_db = """
                PREFIX schema: <https://schema.org/>

                SELECT DISTINCT ?venue
                WHERE {
                    ?publication schema:isPartOf ?venue .
                }
                """
                venues_in_db_df = (get(endpoint, venues_in_db, True)).shape[0]

                new_query = """
                PREFIX schema: <https://schema.org/>
                SELECT DISTINCT ?publication
                WHERE {
                    ?publication schema:identifier ?identifier .
                    FILTER regex(?identifier, "doi")
                }
                """
                result_df = get(endpoint, new_query, True)
                number_of_publications = result_df.shape[0]
                for idx, row in venues_authors_ref.iterrows():
                    #check for each of the dois if they exist in the database:
                    #authors
                    if type(row["orcid"]) == type(a_list):
                        query = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?publication
                        WHERE {{
                            ?publication schema:identifier "{0}" .
                        }}
                        """
                        check_doi = get(endpoint, query.format(row["auth doi"]), True)
                        if check_doi.empty:
                            if row["auth doi"] in publications_possibly_reused:
                                subj = publications_possibly_reused[row["auth doi"]]
                            else:
                                number_of_index = number_of_publications + len(publications_possibly_reused)
                                subj = URIRef(base_url + "publication-" + str(number_of_index))
                                publications_possibly_reused[row["auth doi"]] = subj
                            
                        else:
                            subj = URIRef(check_doi.at[0, "publication"])
                        my_graph.add((subj, doi, Literal(row["auth doi"])))                 
                        for i in range(len(row["orcid"])):
                            auth_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?author
                            WHERE {{
                                ?publication schema:author ?author .
                                ?author schema:identifier "{0}"
                            }}
                            """
                            check_auth = get(endpoint, auth_query.format(row["orcid"][i]), True)   
                            if check_auth.empty:                         
                                if row["orcid"][i] not in authors_dict:
                                    author_subj = URIRef(base_url + "author-" + str(len(authors_dict) + authors_in_db_df))
                                    authors_dict[row["orcid"][i]] = author_subj
                                    my_graph.add((author_subj, doi, Literal(row["orcid"][i])))
                                    my_graph.add((author_subj, name, Literal(row["given name"][i])))
                                    my_graph.add((author_subj, surname, Literal(row["family name"][i])))
                                else: 
                                    author_subj = authors_dict[row["orcid"][i]]
                            else:
                                author_subj = URIRef(check_auth.at[0, "author"])
                            my_graph.add((subj, author, author_subj))
                    #venues
                    query_two = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    check_doi_two = get(endpoint, query_two.format(row["venues doi"]), True)
                    if check_doi_two.empty:
                        if row["venues doi"] in publications_possibly_reused:
                            subj = publications_possibly_reused[row["venues doi"]]
                        else:
                            number_of_index = number_of_publications + len(publications_possibly_reused)
                            subj = URIRef(base_url + "publication-" + str(number_of_index))
                            publications_possibly_reused[row["venues doi"]] = subj
                    else:
                        subj = URIRef(check_doi_two.at[0, "publication"])
                    #I have a subj (a publication): now I need to check if the subj is already associated with a venue
                    if type(row["issn"]) == type(a_list):
                        for i in range(len(row["issn"])):
                            venue_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?venue
                            WHERE {{
                                <{0}> schema:isPartOf ?venue .
                            }}
                            """
                            check_venue = get(endpoint, venue_query.format(subj), True)
                            if check_venue.empty:
                                if row["issn"] in venues_dict:
                                    venue_subj = venues_dict[row["issn"]]
                                    my_graph.add((venue_subj, doi, Literal(row["issn"][i])))
                                else:
                                    venue_subj = URIRef(base_url + "venue-" + str(venues_in_db_df +len(venues_dict)))
                                    venues_dict[row["issn"]] = venue_subj
                                    my_graph.add((venue_subj, doi, Literal(row["issn"][i])))
                                my_graph.add((subj, publicationVenue, venue_subj))
                            else:
                                venue_subj = URIRef(check_venue.at[0, "venue"])
                                my_graph.add((venue_subj, doi, Literal(row["issn"][i])))
                    
                    #citations:
                    query_three= """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    check_doi_three = get(endpoint, query_three.format(row["ref doi"]), True)
                    if check_doi_three.empty:
                        if row["ref doi"] in publications_possibly_reused:
                            subj = publications_possibly_reused[row["ref doi"]] 
                        else: 
                            new_idx = len(publications_possibly_reused)
                            subj = URIRef(base_url + "publication-" + str(new_idx + number_of_publications))
                            my_graph.add((subj, doi, Literal(row["ref doi"])))
                            publications_possibly_reused[row["ref doi"]] = subj
                    if type(row["citation"]) == type(a_list):
                        for w in range(len(row["citation"])):
                            #have I already created a URIRef for the cited publication?
                            query_four= """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?publication
                            WHERE {{
                                ?publication schema:identifier "{0}" .
                            }}
                            """
                            check_doi_four = get(endpoint, query_four.format(row["citation"][w]), True)
                            if check_doi_four.empty:
                                if row["citation"][w] in publications_possibly_reused:
                                    cited_publ = publications_possibly_reused[row["citation"][w]]
                                else:
                                    number_of_index = number_of_publications + len(publications_possibly_reused)
                                    cited_publ = URIRef(base_url + "publication-" + str(number_of_index))
                                    publications_possibly_reused[row["citation"][w]] = subj
                            else:
                                cited_publ = URIRef(check_doi_four.at[0, "publication"])
                            my_graph.add((subj, citation, cited_publ))
                for idx, row in publishers_df.iterrows():
                    pub_query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publisher
                    WHERE {{
                        ?publisher schema:identifier "{0}" .
                    }}
                    """
                    check_pub = get(endpoint, pub_query.format(row["crossref"]), True)
                    if check_pub.empty:
                        num_publisher = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?publisher
                        WHERE {
                            ?publication schema:publisher ?publisher .
                        }
                        """
                        pub_idx = get(endpoint, num_publisher, True).shape[0]
                        subj = URIRef(base_url + "publisher-" + str(pub_idx + len(publisher_dict)))
                        my_graph.add((subj, doi, Literal(row["crossref"])))
                        my_graph.add((subj, title, Literal(row["name"])))
                        publisher_dict[row["crossref"]] = subj
                    else:
                        subj = URIRef(check_pub.at[0, 'publisher'])
                        my_graph.add((subj, title, Literal(row["name"])))                    
        store.open((endpoint, endpoint))
        for triple in my_graph.triples((None, None, None)):
            store.add(triple)
        store.close()
                
class TriplestoreQueryProcessor(TriplestoreProcessor):
    def __init__(self) -> None:
        super().__init__()
    
    #first method: we want to retrieve a DataFrame containing all the publications produced in a certain year, given in input.
    def getPublicationsPublishedInYear(self, year):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publicationInternalId schema:name ?title .
            ?publicationInternalId schema:identifier ?id .
            ?publicationInternalId schema:datePublished ?publicationYear .
            ?publicationInternalId schema:isPartOf ?publicationVenue .
            FILTER (?publicationYear = {0}) .
            OPTIONAL {{?publicationInternalId schema:author ?author }} .
            OPTIONAL {{?publicationInternalId schema:citation ?cites}}
        }}
        """

        publ = get(endpoint, new_query.format(year), True)
        return publ
    
    
    def getPublicationsByAuthorId(self, orcid):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:author ?author .
            ?author schema:identifier ?orcid .
            FILTER (?orcid = "{0}") .
        }}
        """
        publ = get(endpoint, new_query.format(orcid), True)
        a_string = ""
        len_publ = publ.shape[0]
        if len_publ > 0: 
            for idx, row in publ.iterrows():
                if idx == 0:
                    a_string = a_string + "(?id = '" + row["id"] + "')"
                else:
                    a_string = a_string + "|| (?id ='" + row["id"] + "')"
        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            FILTER ({0})
            ?publication schema:author ?author .
            OPTIONAL {{?publication schema:citation ?cites}}
            OPTIONAL {{?publication schema:datePublished ?publicationYear}}
            OPTIONAL {{?publication schema:name ?title}}
            OPTIONAL {{?publication schema:isPartOf ?publicationVenue}}
        }}
        """
        new_publ = get(endpoint, second_query.format(a_string), True)    
        return new_publ     

        
    def getMostCitedPublication(self):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?citation (COUNT(?citation) AS ?cited) 
        WHERE { 
        ?publication schema:citation ?citation .
        }
        GROUP BY ?citation
        ORDER BY desc(?cited)
        """
        publ = get(endpoint, new_query, True)
        a_list = []
        a_string = ""
        if publ.shape[0] > 1:
            a = 0
            b = 1
            most_cited_value = publ.at[0, "cited"]
            a_list.append(publ.at[a, "citation"])
            if publ.at[a, "cited"] == publ.at[b, "cited"]:
                while publ.at[a, "cited"] == publ.at[b, "cited"]:
                    a_list.append(publ.at[b, "citation"])
                    a +=1
                    b +=1
        if len(a_list) == 1:
            a_string = a_string + "(?publication = <" + a_list[0] + ">)"
        elif len(a_list)>1:
            a_string = a_string + "(?publication = <" + a_list[0] + ">)"
            for n in range(len(a_list)-1):
                a_string = a_string + "|| (?publication =<" + a_list[n+1] + ">)"
    
        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            FILTER ({0}) .
            OPTIONAL {{?publication schema:isPartOf ?publicationVenue}} .
            OPTIONAL {{?publication schema:name ?title }} .
            OPTIONAL {{?publication schema:datePublished ?publicationYear }}.
            OPTIONAL {{?publication schema:citation ?cites}}.
            OPTIONAL {{?publication schema:author ?author}}
        }}
        """
        publ_df = get(endpoint, second_query.format(a_string), True)
        publ_df.insert(6, "value_occurrence", most_cited_value)
        return publ_df

    def getMostCitedVenue(self):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?venue (COUNT(?venue) AS ?cited) 
        WHERE { 
        ?publication schema:citation ?citation .
        ?citation schema:isPartOf ?venue
        }
        GROUP BY ?venue
        ORDER BY desc(?cited) 
        """
        venues = get(endpoint, new_query, True)
        a_list = []
        a_string = ""
        if venues.shape[0] > 1:
            a = 0
            b = 1
            a_list.append(venues.at[a, "venue"])
            most_cited_value = venues.at[a, "cited"]
            if venues.at[a, "cited"] == venues.at[b, "cited"]:
                while venues.at[a, "cited"] == venues.at[b, "cited"]:
                    a_list.append(venues.at[b, "venue"])
                    a +=1
                    b +=1
        if len(a_list) == 1:
            a_string = a_string + "(?venue = <" + a_list[0] + ">)"
        elif len(a_list)>1:
            a_string = a_string + "(?venue = <" + a_list[0] + ">)"
            for n in range(len(a_list)-1):
                a_string = a_string + "|| (?venue =<" + a_list[n+1] + ">)"
        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?publisher
        WHERE {{
            ?venue schema:identifier ?id .
            FILTER ({0})
            OPTIONAL {{?venue schema:name ?title }}
            OPTIONAL {{?venue schema:publisher ?publisher}}    
        }}
        """
        venue_df = get(endpoint, second_query.format(a_string), True)
        venue_df.insert(3, "value_occurrence", most_cited_value)
        return venue_df

    
    def getVenuesByPublisherId(self, pub_id):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?publisher
        WHERE {{
            ?venue schema:publisher ?publi .
            ?publi schema:identifier ?publisher .
            FILTER (?publisher = "{0}") . 
            OPTIONAL {{?venue schema:identifier ?id }}.
            OPTIONAL {{?venue schema:name ?title }}.
        }}
        """
        publ = get(endpoint, new_query.format(pub_id), True)
        return publ
    
    def getPublicationInVenue(self, venue):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?publication ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:isPartOf ?publicationVenue .
            ?publicationVenue schema:identifier ?issn .
            FILTER (?issn = "{0}") .
            OPTIONAL {{?publication schema:name ?title}} .
            OPTIONAL {{?publication schema:datePublished ?publicationYear}} .
            OPTIONAL {{?publication schema:author ?author}} .
            OPTIONAL {{?publication schema:citation ?cites}}  .
        }}  
        """
        publ = get(endpoint, new_query.format(venue), True)
        return publ

    def getJournalArticlesInIssue (self, issn, volume ,issue):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites ?volume ?issue
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:isPartOf ?publicationVenue .
            ?publicationVenue schema:identifier ?issn .
            FILTER (?issn = "{0}") .
            ?publication schema:volumeNumber ?volume .
            FILTER (?volume = "{1}").
            ?publication schema:issueNumber ?issue .
            FILTER (?issue = "{2}") . 
            OPTIONAL {{?publication schema:name ?title }}.
            OPTIONAL {{?publication schema:datePublished ?publicationYear .}}
            OPTIONAL {{?publication schema:author ?author}}
            OPTIONAL {{?publication schema:citation ?cites .}}
        }}  
        """
        publ = get(endpoint, new_query.format(issn, volume, issue), True)
        return publ
    
    def getJournalArticlesInVolume(self, issn, volume):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites ?volume ?issue
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:isPartOf ?publicationVenue .
            ?publicationVenue schema:identifier ?issn .
            FILTER (?issn = "{0}") .
            ?publication schema:volumeNumber ?volume .
            FILTER (?volume = "{1}").
            OPTIONAL {{?publication schema:issueNumber ?issue}}.
            OPTIONAL {{?publication schema:name ?title }}.
            OPTIONAL {{?publication schema:datePublished ?publicationYear .}}
            OPTIONAL {{?publication schema:author ?author}}
            OPTIONAL {{?publication schema:citation ?cites .}}
        }}  
        """
        publ = get(endpoint, new_query.format(issn, volume), True)
        return publ
    
    def getJournalArticlesInJournal(self, issn):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites ?volume ?issue
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:isPartOf ?publicationVenue .
            ?publicationVenue schema:identifier ?issn .
            FILTER (?issn = "{0}") .
            OPTIONAL {{?publication schema:issueNumber ?issue }}.
            OPTIONAL {{?publication schema:volumeNumber ?volume }}.
            OPTIONAL {{?publication schema:name ?title }}.
            OPTIONAL {{?publication schema:datePublished ?publicationYear .}}
            OPTIONAL {{?publication schema:author ?author}}
            OPTIONAL {{?publication schema:citation ?cites .}}
        }}  
        """
        publ = get(endpoint, new_query.format(issn), True)
        return publ

    def getPublicationAuthors(self, doi):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?givenName ?familyName
        WHERE {{
            ?publication schema:identifier "{0}" .
            ?publication schema:author ?author .
            ?author schema:identifier ?id .
            ?author schema:givenName ?givenName .
            ?author schema:familyName ?familyName
        }}
        """
        authors = get(endpoint, new_query.format(doi), True)
        return authors
    
    def getPublicationsByAuthorName(self, name):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?publication ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:author ?author .
            ?author schema:givenName ?name .
            ?author schema:familyName ?surname .
            FILTER (regex(?name, "{0}", "i") || regex(?surname, "{1}", "i")).
            OPTIONAL {{?publication schema:name ?title}}.
            OPTIONAL {{?publication schema:isPartOf ?publicationVenue}}
            OPTIONAL {{?publication schema:citation ?cites}}
            OPTIONAL {{?publication schema:author ?author}}
        }} 
        """
        publ = get(endpoint, new_query.format(name, name), True)
        a_list =[]
        a_string= ""
        for n in range(publ.shape[0]):
            a_list.append(publ.at[n, "publication"])
        if len(a_list) == 1:
            a_string = a_string + "(?publication = <" + a_list[0] + ">)"
        elif len(a_list)>1:
            a_string = a_string + "(?publication = <" + a_list[0] + ">)"
            for n in range(len(a_list)-1):
                a_string = a_string + "|| (?publication =<" + a_list[n+1] + ">)"
    
        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            FILTER ({0}) .
            OPTIONAL {{?publication schema:isPartOf ?publicationVenue}} .
            OPTIONAL {{?publication schema:name ?title }} .
            OPTIONAL {{?publication schema:datePublished ?publicationYear }}.
            OPTIONAL {{?publication schema:citation ?cites}}.
            OPTIONAL {{?publication schema:author ?author}}
        }}
        """
        publ_df = get(endpoint, second_query.format(a_string), True)
        return publ_df

    def getProceedingsByEvent(self, eventPartialName):
        endpoint = self.getEndpointUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?title ?publisher ?event
        WHERE {{
            ?publication schema:isPartOf ?venue .
            ?venue schema:identifier ?id .
            ?venue schema:recordedIn ?event .
            FILTER regex(?event, "{0}", "i")
        }}
        """
        venues = get(endpoint, query.format(eventPartialName), True)
        return venues
    
    def getDistinctPublisherOfPublications(self, list_of_pub):
        endpoint = self.getEndpointUrl()
        a_string = ""
        if len(list_of_pub) == 1:
            a_string = a_string + "(?doi = '" + list_of_pub[0] + "')"
        elif len(list_of_pub)>1:
            a_string = a_string + "(?doi = '" + list_of_pub[0] + "')"
            for n in range(len(list_of_pub)-1):
                a_string = a_string + "|| (?doi ='" + list_of_pub[n+1] + "')"
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?publisher ?id ?name
        WHERE {{
            ?publication schema:isPartOf ?venue .
            ?venue schema:publisher ?publisher .
            ?publisher schema:identifier ?id .
            ?publication schema:identifier ?doi .
            FILTER ({0}) .
            OPTIONAL {{?publisher schema:name ?name}}
        }} 
        """
        publ = get(endpoint, new_query.format(a_string), True)
        return publ


rel_path = "relationaldatabase.db"
rel_dp = RelationalDataProcessor()
rel_dp.setdbPath(rel_path)
# rel_dp.uploadData("relational_publications.csv")
# rel_dp.uploadData("relational_other_data.json")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)

#grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
#grp_dp = TriplestoreDataProcessor()
#grp_dp.setEndpointUrl(grp_endpoint)
#grp_dp.uploadData("graph_other_data.json")
#grp_dp.uploadData("relational_publications.csv")
#grp_dp.uploadData("relational_other_data.json")
#grp_dp.uploadData("graph_publications.csv")



#methods queryprocessor!!!!!!!

#grp_qp = TriplestoreQueryProcessor()
#grp_qp.setEndpointUrl(grp_endpoint)
#print(grp_qp.getProceedingsByEvent(""))
#print(grp_qp.getPublicationsPublishedInYear(2013)) #DONE!
#print(grp_qp.getPublicationsByAuthorId("0000-0002-9676-0885")) #DONE!
#print(grp_qp.getMostCitedPublication()) #DONE!
#print(grp_qp.getMostCitedVenue()) #DONE!
#print(grp_qp.getVenuesByPublisherId("crossref:98")) #DONE!
#print(grp_qp.getPublicationInVenue("issn:0138-9130")) #DONE!
#print(grp_qp.getJournalArticlesInIssue("issn:2052-4463", 3, 1))   #DONE!   
#print(grp_qp.getJournalArticlesInVolume("issn:2052-4463", 3))  #DONE!
#print(grp_qp.getJournalArticlesInJournal("issn:2052-4463")) #DONE!
#print(grp_qp.getPublicationAuthors("doi:10.1038/sdata.2016.18")) #DONE!
#print(grp_qp.getPublicationsByAuthorName("Sajja")) #DONE!
#print(grp_qp.getDistinctPublisherOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"])) #DONE!
#Creating a class

rel_qp = RelationalQueryProcessor()
rel_qp.setdbPath(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)

#Now, let's create our classes
class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = [id]

    def getIds(self): #get a list of the ids of the class
        result = []
        for identifier in self.id:
            result.append(identifier)
            result.sort()
            return result

class Person(IdentifiableEntity):
    def __init__(self, id, givenName, familyName):
        self.givenName = givenName
        self.familyName = familyName
        super().__init__(id)   #IMPORTANT: Inherit the parameters/methods/EVERYTHING of the superclass

    def getGivenName(self):  
        return self.givenName   #IMPORTANT: remember return self.<parameter>
  
    def getFamilyName(self):
        return self.familyName


class Publication(IdentifiableEntity):
    def __init__(self, id, author, publicationYear, title, publicationVenue, cites):
        self.author = author #da sistemare
        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue
        self.cites = cites #da sistemare

        super().__init__(id)
    
    
    def getAuthors(self): #da sistemare
        my_person = self.author
        final_set = set()
        for el in my_person:
            if el != None:
                if "http" in el:
                    endpoint = grp_qp.getEndpointUrl()
                    query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?id ?familyName ?givenName
                    WHERE {{
                        ?author schema:identifier ?id .
                        ?author schema:familyName ?familyName .
                        ?author schema:givenName ?givenName .
                        FILTER (?author = <{0}>)
                    }}
                    """
                    result = get(endpoint, query.format(el), True)
                    final_author = Person(result.at[0, "id"], result.at[0, "givenName"], result.at[0, "familyName"])
                    final_set.add(final_author)
                    
                else:
                    for person in el.split(", "):
                        with connect (rel_qp.dbPath) as con:
                            query="SELECT DISTINCT family_name, given_name, orcid FROM Person WHERE auth_id = ?"
                            dframe=read_sql(query,con, params=[person])
                            dframe=dframe.rename(columns={"orcid":"id", "family_name":"familyName", "given_name":"givenName"})
                            if len(dframe)>1:
                                data = dframe.iloc[0]
                            else:
                                data = dframe
                        final_author= Person(id=data["id"], familyName=data["familyName"], givenName=data["givenName"])
                        final_set.add(final_author)
        return final_set
    
    def getPublicationYear(self):
        return self.publicationYear
    
    def getTitle(self):
        return self.title
    
    def getCitedPublications(self): #da sistemare
        result= []
        a_dict= {}
        for el in self.cites:
            if type(el) == type("str"):
                if "http" in el:
                    endpoint = grp_qp.getEndpointUrl()
                    query = """
                    PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX schema: <https://schema.org/>
                    SELECT ?id ?title ?publicationYear ?publicationVenue ?author ?cites
                    WHERE {{
                        ?publication schema:identifier ?id .
                        FILTER (?publication = <"{0}">) .
                        OPTIONAL {{?publication schema:isPartOf ?publicationVenue}} .
                        OPTIONAL {{?publication schema:name ?title }} .
                        OPTIONAL {{?publication schema:datePublished ?publicationYear }}.
                        OPTIONAL {{?publication schema:citation ?cites}}.
                        OPTIONAL {{?publication schema:author ?author}}
                    }}
                    """
                    a_publ = get(endpoint, query.format(el), True)
                    
                    for idx, row in a_publ.iterrows():
                        if row["id"] not in a_dict:
                            a_dict[row["id"]] = {
                                "id": row["id"],
                                "title" :row["title"],
                                "publicationYear" : row["publicationYear"],
                                "publicationVenue" : row["publicationVenue"],
                                "authors" : [row["author"]],
                                "cites" : [row["cites"]]
                            }
                        else:
                            if row["author"] not in (a_dict[row["id"]])["authors"]:
                                (a_dict[row["id"]])["authors"].append(row["author"])
                            if row["cites"] not in (a_dict[row["id"]])["cites"] and type(row["cites"]) == type("str"):
                                (a_dict[row["id"]])["cites"].append(row["cites"])
                    for el in a_dict:
                        result.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
                        return result
                    
                else:
                    final_set=set()
                    if el != None:
                        for ref in el.split(", "):
                            with connect (rel_qp.dbPath) as con:
                                query="""SELECT DISTINCT "id", "title", "publication_year", "publication_venue", "author", "cites"
                                FROM "References" 
                                LEFT JOIN "Publications"
                                ON "References".'ref doi' = "Publications".'id'
                                LEFT JOIN Authors_Obj
                                ON Publications.id = Authors_Obj.auth_doi
                                LEFT JOIN References_Obj
                                ON Publications.id = References_Obj.ref_doi
                                WHERE ref_id = ? """
                                dframe = read_sql(query,con, params=[ref])
                                dframe = dframe.rename(columns={"publication_year":"publicationYear", "publication_venue":"publicationVenue"})            
                                data = dframe.iloc[:1]
                            final_ref= Publication(id=data["id"], title=data["title"], publicationYear=data["publicationYear"], publicationVenue=data["publicationVenue"], author=data["author"], cites=data["cites"])
                            final_set.add(final_ref)
                    return final_set
    
    
    def getPublicationVenue(self):
        if self.publicationVenue != None:  #added this in beginning, returns error otherwise 
            if "http" in self.publicationVenue:
                endpoint = grp_qp.getEndpointUrl()
                new_query = """
                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>

                SELECT ?id ?title ?publisher
                WHERE {{
                    ?venue schema:identifier ?id .
                    FILTER (?venue = <{0}>) .
                    OPTIONAL {{?venue schema:name ?title }}
                    OPTIONAL {{?venue schema:publisher ?publisher}}    
                }}
                """
                publ = get(endpoint, new_query.format(self.publicationVenue), True)
                if publ.shape[0] == 2:
                    issn = []
                    for idx,row in publ.iterrows():
                        issn.append([row["id"]])
                        title = row["title"]
                        publisher = row["publisher"]
                    venue = Venue(issn, title, publisher)
                elif publ.shape[0] == 1:
                    for idx, row in publ.iterrows():
                        issn = row["id"]
                        title = row["title"]
                        publisher = row["publisher"]
                    venue = Venue(issn, title, publisher)
                elif publ.shape[0] == 0:
                    venue = None
                return venue

            else:

                    with connect (rel_qp.dbPath) as con:
                        query="""SELECT DISTINCT "issn", "title", "publisher"
                        FROM Publications 
                        LEFT JOIN Global_json_dataframe 
                        ON Publications.id = Global_json_dataframe.doi 
                        WHERE title = ? """
                        dframe = read_sql(query,con, params=[self.publicationVenue])
                        dframe = dframe.rename(columns={"issn":"id"})
                        final_ven= Venue(id=dframe["id"], title=dframe["title"], publisher=dframe["publisher"])
                    return final_ven

 #***VENUES***   

class Venue(IdentifiableEntity):
    def __init__(self, id, title, publisher):
        self.title = title 
        self.publisher = publisher

        super().__init__(id)
    
    def getTitle(self):
        return self.title
    
    def getPublisher(self):
        if "http" in self.publisher: 
            endpoint = grp_qp.getEndpointUrl()
            new_query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?id ?title ?publisher
            WHERE {{
                ?venue schema:publisher ?organization .
                FILTER (?organization = <{0}>) .
                OPTIONAL {{?organization schema:name ?orgName }}
                OPTIONAL {{?organization schema:identifier ?orgID}}    
            }}
            """
            publ = get(endpoint, new_query.format(self.publisher), True)
            final_org = Organization(publ.at[0, "orgID"], publ.at[0, "orgName"])
        else:
            if self.publisher != None:
                with connect (rel_qp.dbPath) as con:
                    query="""SELECT DISTINCT "publisher id", "name"
                    FROM Publishers
                    WHERE "publisher id" = ? """
                    dframe = read_sql(query,con, params=[self.publisher])
                    dframe = dframe.rename(columns={"publisher id":"id"})
                    final_org= Organization(id=dframe["id"], name=dframe["name"])
            return final_org

class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name


class JournalArticle(Publication): #it's a type of publication with 2 extras
    def __init__(self, id, author, title, publicationYear, publicationVenue, cites, issue, volume):
        self.issue = issue
        self.volume = volume
        # IMPORTANT: Here is where the constructor of the superclass is explicitly recalled, so as
        # to handle the input parameters as done in the superclass
        super().__init__(id, author, title, publicationYear, publicationVenue, cites)  
    
    def getIssue(self):
        return self.issue
    
    def getVolume(self):
        return self.volume


class BookChapter(Publication): #it is a publication that just inherits its parameters
    def __init__(self, id, author, publicationYear, title, publicationVenue, cites, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(id, author, publicationYear, title, publicationVenue, cites)  

    def getChapterNumber(self):
        return self.chapterNumber

class ProceedingsPaper(Publication):
    pass


class Journal(Venue):
    pass


class Book(Venue):
    pass

class Proceedings(Venue):
    def __init__(self, id, title, publisher, event):
        self.event = event
        super().__init__(id, title, publisher)  
  

    def getEvent(self):
        return self.event







from pandas import concat
class GenericQueryProcessor(object):   #THIS TAKES NO LIST AS INPUT
    def __init__(self):
        self.queryProcessor = []
        
    def cleanQueryProcessors(self):
        if len(self.queryProcessor) > 0:
            self.queryProcessor = []
            
    def addQueryProcessor(self, QueryProcessor):
        self.QueryProcessor=QueryProcessor
        self.queryProcessor.append(self.QueryProcessor)
            
    def getPublicationsPublishedInYear(self, year):
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getPublicationsPublishedInYear(year)
            a_dict = {}
            listOfPublication = []
            a_string = ""
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getPublicationsPublishedInYear(year)],ignore_index = True, axis = 0)
            for idx, row in part_query.iterrows():
                if row["id"] not in a_dict:
                    a_dict[row["id"]] = {
                        "id": row["id"],
                        "title" :row["title"],
                        "publicationYear" : row["publicationYear"],
                        "publicationVenue" : row["publicationVenue"],
                        "authors" : [row["author"]],
                        "cites" : [row["cites"]]
                    }
                else:
                    if row["author"] not in (a_dict[row["id"]])["authors"]:
                        (a_dict[row["id"]])["authors"].append(row["author"])
                    if row["cites"] not in (a_dict[row["id"]])["cites"] and type(row["cites"]) == type(a_string):
                        (a_dict[row["id"]])["cites"].append(row["cites"])
                   
            for el in a_dict:
                listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
            return listOfPublication
    
    def getPublicationsByAuthorId(self, id):
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getPublicationsByAuthorId(id)
            a_dict = {}
            listOfPublication = []
            a_string = ""
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getPublicationsByAuthorId(id)],ignore_index = True, axis = 0)
            for idx, row in part_query.iterrows():
                if row["id"] not in a_dict:
                    a_dict[row["id"]] = {
                        "id": row["id"],
                        "title" :row["title"],
                        "publicationYear" : row["publicationYear"],
                        "publicationVenue" : row["publicationVenue"],
                        "authors" : [row["author"]],
                        "cites" : [row["cites"]]
                    }
                else:
                    if row["author"] not in (a_dict[row["id"]])["authors"]:
                        (a_dict[row["id"]])["authors"].append(row["author"])
                    if row["cites"] not in (a_dict[row["id"]])["cites"] and type(row["cites"]) == type(a_string):
                        (a_dict[row["id"]])["cites"].append(row["cites"])
                   
            for el in a_dict:
                listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
                for item in listOfPublication:
                    sublist=[]
                    sublist.append(item.getIds())
                    sublist.append(item.getAuthors())
                    sublist.append(item.getTitle())
                    sublist.append(item.getPublicationYear())
                    sublist.append(item.getPublicationVenue())
                    sublist.append(item.getCitedPublications())
                    print (sublist)
            return listOfPublication
        
    def getMostCitedPublication(self): #NON FUNZIONA MANCA AUTHOR NEL RELATIONAL
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getMostCitedPublication()
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getMostCitedPublication()],ignore_index = True, axis = 0)
            sorted_query=part_query.sort_values(by='value_occurrence', ascending=False, ignore_index=True)
            if len(sorted_query)>1:
                col_ind=6
                result_df=DataFrame()
                for idx, row in sorted_query.iterrows():
                    if idx==0:
                        result_df=concat([sorted_query.loc[[0]]])
                    if idx>0:
                        if sorted_query.iloc[idx, col_ind]==sorted_query.iloc[idx-1, col_ind]:
                            result_df=concat([sorted_query.loc[[idx]]])
                        if sorted_query.iloc[idx, col_ind]<sorted_query.iloc[idx-1, col_ind]:
                            result_df
            else:
                result_df=sorted_query
            a_dict = {}
            listOfPublication = []
            a_string = ""
            for idx, row in result_df.iterrows():
                if row["id"] not in a_dict:
                    a_dict[row["id"]] = {
                        "id": row["id"],
                        "title" :row["title"],
                        "publicationYear" : row["publicationYear"],
                        "publicationVenue" : row["publicationVenue"],
                        "authors" : [row["author"]],
                        "cites" : [row["cites"]]
                    }
                else:
                    if row["author"] not in (a_dict[row["id"]])["authors"]:
                        (a_dict[row["id"]])["authors"].append(row["author"])
                    if row["cites"] not in (a_dict[row["id"]])["cites"] and type(row["cites"]) == type(a_string):
                        (a_dict[row["id"]])["cites"].append(row["cites"])
                   
            for el in a_dict:
                listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
                for item in listOfPublication:
                    sublist=[]
                    sublist.append(item.getIds())
                    sublist.append(item.getAuthors())
                    sublist.append(item.getTitle())
                    sublist.append(item.getPublicationYear())
                    sublist.append(item.getPublicationVenue())
                    sublist.append(item.getCitedPublications())
                    print (sublist)
            return listOfPublication
        
    def getMostCitedVenue(self): #CONFRONTO CON COUNT DEL TRIPLESTORE MA COME? SI PUO' VISUALIZZARE IL COUNT DEL TRIPLESTORE? QUELLO DEL RELATIONAL SI'!
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getMostCitedVenue()
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getMostCitedVenue()],ignore_index = True, axis = 0)
            sorted_query=part_query.sort_values(by='value_occurrence', ascending=False, ignore_index=True)
            if len(sorted_query)>1:
                col_ind=3
                result_df=DataFrame()
                for idx, row in sorted_query.iterrows():
                    if idx==0:
                        result_df=concat([sorted_query.loc[[0]]])
                    if idx>0:
                        if sorted_query.iloc[idx, col_ind]==sorted_query.iloc[idx-1, col_ind]:
                            result_df=concat([sorted_query.loc[[idx]]])
                        if sorted_query.iloc[idx, col_ind]<sorted_query.iloc[idx-1, col_ind]:
                            result_df=result_df
            else:
                result_df=sorted_query
            a_dict = {}
            listOfPublication = []
            a_string = ""
            for idx, row in result_df.iterrows():
                if row["title"] not in a_dict:
                    a_dict[row["id"]] = {
                        "id": [row["id"]],
                        "title" :row["title"],
                        "publisher" : row["publisher"],
                    }
                else:
                    if row["id"] not in a_dict[row["title"]]["id"]:
                        a_dict[row["title"]]["id"].append(row["id"])
                    if row["publisher"] not in (a_dict[row["title"]])["publisher"]:
                        (a_dict[row["title"]])["publisher"].append(row["publisher"])
                   
            for el in a_dict:
                listOfPublication.append((Venue(a_dict[el]["id"], a_dict[el]["title"], a_dict[el]["publisher"])))
                for item in listOfPublication:
                    sublist=[]
                    sublist.append(item.getIds())
                    sublist.append(item.getTitle())
                    sublist.append(item.getPublisher())
                    print (sublist)
            return listOfPublication
        
    def getVenuesByPublisherId(self, id):
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getVenuesByPublisherId(id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getVenuesByPublisherId(id)],ignore_index = True, axis = 0)
        a_dict = {}
        listOfPublication = []
        a_string = ""
        for idx, row in part_query.iterrows():
            if row["title"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": [row["id"]],
                    "title" :row["title"],
                    "publisher" : row["publisher"],
                }
            else:
                if row["id"] not in a_dict[row["title"]]["id"]:
                    a_dict[row["title"]]["id"].append(row["id"])
                if row["publisher"] not in (a_dict[row["title"]])["publisher"]:
                    (a_dict[row["title"]])["publisher"].append(row["publisher"])
                
        for el in a_dict:
            listOfPublication.append((Venue(a_dict[el]["id"], a_dict[el]["title"], a_dict[el]["publisher"])))
            for item in listOfPublication:
                sublist=[]
                sublist.append(item.getIds())
                sublist.append(item.getTitle())
                sublist.append(item.getPublisher())
                print (sublist)
        return listOfPublication
        
    def getPublicationInVenue(self, id):
        a_dict = {}
        listOfPublication = []
        a_string = "" 
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getPublicationInVenue(id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getPublicationInVenue(id)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" : row["publicationVenue"],
                    "authors" : [row["author"]],
                    "cites" : [row["cites"]]
                }
            else:
                if row["author"] not in (a_dict[row["id"]])["authors"]:
                    (a_dict[row["id"]])["authors"].append(row["author"])
                if row["cites"] not in (a_dict[row["id"]])["cites"] and type(row["cites"]) == type(a_string):
                    (a_dict[row["id"]])["cites"].append(row["cites"])
                
        for el in a_dict:
            listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
        return listOfPublication
    
    def getJournalArticlesInIssue(self, issue, volume, id):
        a_dict = {}
        listOfPublication = []
        a_string = "" 
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getJournalArticlesInIssue(issue, volume, id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getJournalArticlesInIssue(issue, volume, id)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" : row["publicationVenue"],
                    "authors" : [row["author"]],
                    "cites" : [row["cites"]], 
                    "volume" : row["volume"],
                    "issue" :row["issue"]
                }
            else:
                if row["author"] not in (a_dict[row["id"]])["authors"]:
                    (a_dict[row["id"]])["authors"].append(row["author"])
                if row["cites"] not in (a_dict[row["id"]])["cites"] and type(row["cites"]) == type(a_string):
                    (a_dict[row["id"]])["cites"].append(row["cites"])
                
        for el in a_dict:
            listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"], a_dict[el]["volume"], a_dict[el]["issue"])))
        return listOfPublication
        
    def getJournalArticlesInVolume(self, volume, id):
        a_dict = {}
        listOfPublication = []
        a_string = "" 
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getJournalArticlesInVolume(volume, id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getJournalArticlesInVolume(volume, id)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" : row["publicationVenue"],
                    "authors" : [row["author"]],
                    "cites" : [row["cites"]],
                    "volume" : row["volume"],
                    "issue" :row["issue"]
                }
            else:
                if row["author"] not in (a_dict[row["id"]])["authors"]:
                    (a_dict[row["id"]])["authors"].append(row["author"])
                if row["cites"] not in (a_dict[row["id"]])["cites"] and type(row["cites"]) == type(a_string):
                    (a_dict[row["id"]])["cites"].append(row["cites"])
                
        for el in a_dict:
            listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"], a_dict[el]["volume"], a_dict[el]["issue"])))
        return listOfPublication
        
    def getJournalArticlesInJournal(self, id):
        a_dict = {}
        listOfPublication = []
        a_string = ""  
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getJournalArticlesInJournal(id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getJournalArticlesInJournal(id)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" : row["publicationVenue"],
                    "authors" : [row["author"]],
                    "cites" : [row["cites"]],
                    "volume" : row["volume"],
                    "issue" :row["issue"]
                }
            else:
                if row["author"] not in (a_dict[row["id"]])["authors"]:
                    (a_dict[row["id"]])["authors"].append(row["author"])
                if row["cites"] not in (a_dict[row["id"]])["cites"] and type(row["cites"]) == type(a_string):
                    (a_dict[row["id"]])["cites"].append(row["cites"])
                
        for el in a_dict:
            listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"], a_dict[el]["volume"], a_dict[el]["issue"])))
        return listOfPublication
    
        
    def getProceedingsByEvent(self, event):
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getProceedingsByEvent(event)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getProceedingsByEvent(event)],ignore_index = True, axis = 0)
        a_dict = {}
        listOfPublication = []
        a_string = ""
        for idx, row in part_query.iterrows():
            if row["title"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": [row["id"]],
                    "title" :row["title"],
                    "publisher" : row["publisher"],
                    "event" : row["event"]
                }
            else:
                if row["id"] not in a_dict[row["title"]]["id"]:
                    a_dict[row["title"]]["id"].append(row["id"])
                if row["publisher"] not in (a_dict[row["title"]])["publisher"]:
                    (a_dict[row["title"]])["publisher"].append(row["publisher"])
                
        for el in a_dict:
            listOfPublication.append((Venue(a_dict[el]["id"], a_dict[el]["title"], a_dict[el]["publisher"], a_dict[el]["event"])))
        return listOfPublication
            
    
    def getPublicationAuthors(self, id):
        a_dict = {}
        listOfPublication = []
        a_string = "" 
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getPublicationAuthors(id)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "familyName" :row["familyName"],
                    "givenName" : row["givenName"],
                }
        for el in a_dict:
            listOfPublication.append((Person(a_dict[el]["id"], a_dict[el]["givenName"], a_dict[el]["familyName"])))
        return listOfPublication

    def getPublicationsByAuthorName(self, name):
        a_dict = {}
        listOfPublication = []
        a_string = ""   
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getPublicationsByAuthorName(name)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getPublicationsByAuthorName(name)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" : row["publicationVenue"],
                    "authors" : [row["author"]],
                    "cites" : [row["cites"]]
                }
            else:
                if row["author"] not in (a_dict[row["id"]])["authors"]:
                    (a_dict[row["id"]])["authors"].append(row["author"])
                if row["cites"] not in (a_dict[row["id"]])["cites"] and type(row["cites"]) == type(a_string):
                    (a_dict[row["id"]])["cites"].append(row["cites"])
                
        for el in a_dict:
            listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
        print(listOfPublication[0].getIds())
        print(listOfPublication[0].getAuthors())
        return listOfPublication
        
    def getDistinctPublisherOfPublications(self, doi):
        a_dict = {}
        listOfPublication = []
        a_string = ""    
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getDistinctPublisherOfPublications(doi)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "name" :row["name"],
                }
            else:
                if type(row["name"]) == type("str") and row["name"] not in (a_dict[row["id"]]):
                    (a_dict[row["id"]])["name"] = row["name"]
        for el in a_dict:
            listOfPublication.append((Organization(a_dict[el]["id"], a_dict[el]["name"])))
        return listOfPublication
    
#gen_query = GenericQueryProcessor()
#gen_query.cleanQueryProcessors()
#gen_query.addQueryProcessors(rlp_query)
#gen_query.addQueryProcessors(grp_qp)
#gen_query.getPublicationsPublishedInYear(2020) #OK
#print(gen_query.getPublicationsByAuthorId("0000-0001-7412-4776"))
#print(gen_query.getMostCitedPublication())
#print(gen_query.getMostCitedVenue())
#print(gen_query.getVenuesByPublisherId("crossref:2373")) #OK
#print(gen_query.getPublicationInVenue("issn:2164-551")) #OK
#print(gen_query.getJournalArticlesInIssue("1", "12", "issn:1758-2946")) #OK
#print(gen_query.getJournalArticlesInVolume("17", "issn:2164-5515")) #OK 
#print(gen_query.getJournalArticlesInJournal("issn:2164-551")) #OK 
#gen_query.getProceedingsByEvent("web") 
#print(gen_query.getPublicationAuthors("doi:10.1080/21645515.2021.1910000")) #OK
#print(gen_query.getPublicationsByAuthorName("David"))
#print(gen_query.getDistinctPublisherOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"]))

