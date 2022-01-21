from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate, pprintAlgebra, Filter
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate
from rdflib.plugins.sparql.evaluate import evalFilter
from rdflib.plugins.sparql.parserutils import CompValue, Expr


def ppp(p):
      if not isinstance(p, CompValue):
            #print(p)
            return
      print("%s(" %(p.name,))

      name = "%s(" %(p.name,)
      name1 = "Filter("
      name2 = "OrderBy("

      if name == name1 :
            print("OKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK")

      if name == name2 :
            print("OKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK")

      for k in p:
            #print("" % (),)
                

            ppp(p[k])
  



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

query1 = 'SELECT ?var1 WHERE {  ?var1  <http://www.w3.org/2004/02/skos/core#altLabel>  ?var2 . FILTER (  (  REGEX (  ?var2 , "string1" )  ) ) .}'
query2 = '''
SELECT ?var1  ?var1Label  ?var2 (  CONCAT (  ""string1"", REPLACE (  STR (  ?var3  ) , ""string2"", """" ) , ""]]"" )  AS  ?var4  ) ?var2Label  ?var5 
WHERE {  <http://www.bigdata.com/queryHints#Query>  <http://www.bigdata.com/queryHints#optimizer>  ""None"".  
?var2  <http://wikiba.se/ontology#propertyType>  <http://wikiba.se/ontology#Monolingualtext> .  
?var2  <http://wikiba.se/ontology#claim>  ?var3 .  ?var1  ?var3  ?var6 .  
?var2  <http://wikiba.se/ontology#statementProperty>  ?var7 .  
?var6  ?var7  ?var5 . FILTER (  ( (  LANG (  ?var5  )  =  ""und"" ) ) ) . 
FILTER (  ( (  ?var1  !=  <http://www.wikidata.org/entity/Q22282914>  ) ) ) . 
SERVICE  <http://wikiba.se/ontology#label>   {    <http://www.bigdata.com/rdf#serviceParam>  <http://wikiba.se/ontology#language>  ""en"".  
}}ORDER BY  DESC( ?var2Label )ASC( ?var5 )LIMIT 5000'''

logical_plan = translateQuery(parseQuery(query2)).algebra
#pprintAlgebra(translateQuery(parseQuery(query)))

ppp(logical_plan)

#print(logical_plan)
