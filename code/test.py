from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate

query = '''
SELECT ?var1  ?var1Label  ?var2 (  CONCAT (  "string1", REPLACE (  STR (  ?var3  ) , "string2", "" ) , "]]" )  AS  ?var4  ) ?var2Label  ?var5 
WHERE {  <http://www.bigdata.com/queryHints#Query>  <http://www.bigdata.com/queryHints#optimizer>  "None".  
       ?var2  <http://wikiba.se/ontology#propertyType>  <http://wikiba.se/ontology#Monolingualtext> .  
       ?var2  <http://wikiba.se/ontology#claim>  ?var3 .  
       ?var1  ?var3  ?var6 .  
       ?var2  <http://wikiba.se/ontology#statementProperty>  ?var7 .  
       ?var6  ?var7  ?var5 . FILTER (  ( (  LANG (  ?var5  )  =  "und" ) ) ) . 
       FILTER (  ( (  ?var1  !=  <http://www.wikidata.org/entity/Q22282914>  ) ) ) . 
       SERVICE  <http://wikiba.se/ontology#label>   {    <http://www.bigdata.com/rdf#serviceParam>  <http://wikiba.se/ontology#language>  "en".  
                                                    }
      }ORDER BY  DESC( ?var2Label )ASC( ?var5 )LIMIT 5000	'''
logical_plan = translateQuery(parseQuery(query)).algebra
print(logical_plan)
