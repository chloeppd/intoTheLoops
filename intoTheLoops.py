
# 1) Importing all the classes for handling the relational database
from DMClasses import RelationalDataProcessor, RelationalQueryProcessor

# 2) Importing all the classes for handling RDF database
from DMClasses import TriplestoreDataProcessor, TriplestoreQueryProcessor

# 3) Importing the class for dealing with generic queries
from DMClasses import GenericQueryProcessor

# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relationaldatabase.db"
rel_dp = RelationalDataProcessor()
rel_dp.setdbPath(rel_path)
rel_dp.uploadData("relational_publications.csv")
rel_dp.uploadData("relational_other_data.json")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
#grp_dp.uploadData("graph_publications.csv")
#grp_dp.uploadData("graph_other_data.json")

# In the next passage, create the query processors for both
# the databases, using the related classes
lala = RelationalQueryProcessor()
lala.setdbPath(rel_path)

lili = TriplestoreQueryProcessor()
lili.setEndpointUrl(grp_endpoint)

# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(lala)
generic.addQueryProcessor(lili)

result_q1 = generic.getPublicationsPublishedInYear(2016)
print(result_q1)

#print(generic.getPublicationsByAuthorId("0000-0001-7412-4776"))
#print(generic.getMostCitedPublication())
#print(generic.getMostCitedVenue())
#print(generic.getVenuesByPublisherId("crossref:2373")) #OK
#print(generic.getPublicationInVenue("issn:2164-551")) #OK
#print(generic.getJournalArticlesInIssue("1", "12", "issn:1758-2946")) #OK
#print(generic.getJournalArticlesInVolume("17", "issn:2164-5515")) #OK 
#print(generic.getJournalArticlesInJournal("issn:2164-551")) #OK 
#generic.getProceedingsByEvent("web") 
#print(generic.getPublicationAuthors("doi:10.1080/21645515.2021.1910000")) #OK
#print(generic.getPublicationsByAuthorName("david"))
#print(generic.getDistinctPublisherOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"]))

