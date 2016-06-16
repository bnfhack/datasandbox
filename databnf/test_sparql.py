import unittest


from sparql import autoprefix, DatabnfDatabase


class AutoPrefixTests(unittest.TestCase):

    def test_autoprefix(self):
        query = autoprefix('''SELECT ?pref WHERE {
    ?concept a skos:Concept;
             skos:prefLabel ?pref;
             foaf:focus ?obj.
    ?obj a foaf:Organization.
    }''', DatabnfDatabase.namespaces)
        self.assertEqual('''PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?pref WHERE {
    ?concept a skos:Concept;
             skos:prefLabel ?pref;
             foaf:focus ?obj.
    ?obj a foaf:Organization.
    }''', query)



if __name__ == '__main__':
    unittest.main()
