"""helper to query sparql endpoints and databnf endpoint specifically"""

import logging
import os
import os.path as osp
import json
import re
from functools import wraps
from itertools import chain
from collections import namedtuple
import hashlib

from SPARQLWrapper import SPARQLWrapper, JSON


try:
    text_type = unicode
except NameError:  # py3
    text_type = str


class Literal(text_type):
    def __new__(cls, value, lang=None):
        inst = super(Literal, cls).__new__(cls, value)
        inst.lang = lang
        return inst


class SparqlRset(object):

    def __init__(self, results):
        self._results = results
        self.fieldnames = results['head']['vars']
        self.row_cls = namedtuple('SparqlRsetRow', self.fieldnames)

    def __iter__(self):
        """iterates on results of a SPARQL query saved as json by virtuoso.

        yields:
          A namedtuple where each attribute maps one of the sparql variable.
          Values are not postprocessed.
        """
        for rowdef in self._results['results']['bindings']:
            # XXX use rdflib.term objects ?
            data = []
            for field in self.fieldnames:
                if field not in rowdef:
                    fieldvalue = None
                else:
                    fieldvalue = rowdef[field]['value']
                    if rowdef[field]['type'] == 'literal':
                        fieldvalue = Literal(fieldvalue,
                                             lang=rowdef[field].get('xml:lang'))
                    if rowdef[field]['type'] == 'typed-literal':
                        if rowdef[field]['datatype'] == 'http://www.w3.org/2001/XMLSchema#integer':
                            fieldvalue = int(fieldvalue)
                data.append(fieldvalue)
            yield self.row_cls(*data)

    def __len__(self):
        return len(self._results['results']['bindings'])


def cacheresults(execute):
    """cache sparql results in a file to avoid sending query next time"""
    @wraps(execute)
    def _execute(self, query):
        if not self.cache_dir:
            return execute(self, query)
        sha = hashlib.sha256(query).hexdigest()
        cache_filepath = osp.join(self.cache_dir, '{}.json'.format(sha))
        if osp.isfile(cache_filepath):
            with open(cache_filepath) as cache_f:
                return json.load(cache_f)
        result = execute(self, query)
        with open(cache_filepath, 'w') as cache_f:
            json.dump(result, cache_f, indent=2)
        return result
    return _execute


class SPARQLDatabase(object):
    """wrapper around sparqlwrapper api.

    Results can be accessed either by index or by bindings.

    >>> db = SPARQLDatabase('http://data.bnf.fr/sparql')
    >>> results = db.execute('SELECT * WHERE {?s ?p ?o} LIMIT 10')
    >>> for r in results:
    >>>     print(r.s, r.p, r.o)
    >>>     print(r[0], r[1], r[2])
    >>>

    NOTE: if ``cache_dir`` is used, result bindings are lost, only numeric
    index can be used to access results. This is a known limitation that will
    be adressed later
    """
    def __init__(self, endpoint, cache_dir=None):
        self.querier = SPARQLWrapper(endpoint)
        self.querier.setReturnFormat(JSON)
        self.cache_dir = cache_dir
        if cache_dir and not osp.isdir(cache_dir):
            os.makedirs(cache_dir)

    @cacheresults
    def _execute(self, query):
        self.querier.setQuery(query)
        try:
            return self.querier.query().convert()
        except:
            logging.exception('failed to execute SPARQL query %r',
                              query)
            return {
                'head': {'vars': ()},
                'results': {'bindings': ()},
            }

    def execute(self, query):
        raw_results = self._execute(query)
        return SparqlRset(raw_results)


class DatabnfDatabase(SPARQLDatabase):
    """provide specific utilities for data.bnf.fr querying

    >>> db = DatabnfDatabase()
    >>> props = db.fetch_authority_infos(11917290)
    >>> print(props['skos:altLabel'])
    'Alfredo de Musset (1810-1857)
    >>> print(len(props['foaf:depiction']))
    48
    >>> print([url for url in props['foaf:depiction'] if 'wikimedia' in url])
    [u'http://commons.wikimedia.org/wiki/Special:FilePath/Alfred_de_musset.jpg',
     u'http://commons.wikimedia.org/wiki/Special:FilePath/Alfred_de_musset.jpg?width=300']
    """

    namespaces = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'skos': 'http://www.w3.org/2004/02/skos/core#',
        'foaf': 'http://xmlns.com/foaf/0.1/',
        'xfoaf': 'http://www.foafrealm.org/xfoaf/0.1/',
        'dcmitype': 'http://purl.org/dc/dcmitype/',
        'foaf': 'http://xmlns.com/foaf/0.1/',
        'ore': 'http://www.openarchives.org/ore/terms/',
        'ark': 'http://ark.bnf.fr/ark:/12148/',
        'dbpedia': 'http://dbpedia.org/',
        'dbpediaowl': 'http://dbpedia.org/ontology/',
        'dbprop': 'http://dbpedia.org/property/',
        'rdagroup2elements': 'http://rdvocab.info/ElementsGr2/',
        'frbr': 'http://rdvocab.info/uri/schema/FRBRentitiesRDA/',
        'rdarole': 'http://rdvocab.info/roles/',
        'rdagroup1elements': 'http://rdvocab.info/Elements/',
        'rdarelationships': 'http://rdvocab.info/RDARelationshipsWEMI/',
        'og': 'http://ogp.me/ns#',
        'bnf-onto': 'http://data.bnf.fr/ontology/bnf-onto/',
        'dcterms': 'http://purl.org/dc/terms/',
        'owl': 'http://www.w3.org/2002/07/owl#',
        'time': 'http://www.w3.org/TR/owl-time/',
        'marcrel': 'http://id.loc.gov/vocabulary/relators/',
        'bnfroles': 'http://data.bnf.fr/vocabulary/roles/',
        'mo': 'http://musicontology.com/',
        'geo': 'http://www.w3.org/2003/01/geo/wgs84_pos#',
        'ign': 'http://data.ign.fr/ontology/topo.owl/',
        'insee': 'http://rdf.insee.fr/geo/',
        'gn': 'http://www.geonames.org/ontology/ontology_v3.1.rdf/',
        'dcdoc': 'http://dublincore.org/documents/',
        'bio': 'http://vocab.org/bio/0.1/',
        'isni': 'http://isni.org/ontology#',
        'bibo': 'http://purl.org/ontology/bibo/',
        'schema': 'http://schema.org/',
    }

    concept_props = u'''
PREFIX bnf-onto: <http://data.bnf.fr/ontology/bnf-onto/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?concept ?prop ?value WHERE {
 ?concept bnf-onto:FRBNF %(bnf_id)s;
    ?prop ?value.

}
    '''

    foaf_props = u'''
PREFIX bnf-onto: <http://data.bnf.fr/ontology/bnf-onto/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?agent ?prop ?value WHERE {
 ?concept bnf-onto:FRBNF %(bnf_id)s;
    foaf:focus ?agent.
 ?agent ?prop ?value.
}
    '''

    def __init__(self, cache_dir=None):
        super(DatabnfDatabase, self).__init__('http://data.bnf.fr/sparql',
                                              cache_dir=cache_dir)

    def fetch_authority_infos(self, bnf_id):
        infos = {}
        results = chain(self.execute(self.concept_props % {'bnf_id': bnf_id}),
                        self.execute(self.foaf_props % {'bnf_id': bnf_id}))
        for _, prop, value in results:
            prop = ns_prop(prop, self.namespaces)
            if prop in infos:
                if not isinstance(infos[prop], list):
                    infos[prop] = [infos[prop]]
                infos[prop].append(value)
            else:
                infos[ns_prop(prop, self.namespaces)] = value
        return infos

    def execute(self, query):
        query = autoprefix(query, self.namespaces)
        return super(DatabnfDatabase, self).execute(query)


def ns_prop(uri, namespaces):
    """convert the predicate ``uri`` in a readable qualified name

    >>> ns_prop('http://www.w3.org/2004/02/skos/core#prefLabel',
    ...         DatabnfDatabase.namespaces)
    'skos:prefLabel'
    """
    reverse_ns = {v: k for k, v in namespaces.items()}
    uri_lookup = sorted(reverse_ns.keys(),
                        key=lambda k: len(k),
                        reverse=True)
    for ns_uri in uri_lookup:
        if uri.startswith(ns_uri):
            ns = reverse_ns[ns_uri]
            return u'{}:{}'.format(ns, uri[len(ns_uri):])
    return uri


def autoprefix(query, namespaces):
    """tries to automatically add PREFIX declaration for ``query``

    PREFIX uris are found in ``namespaces``.
    """
    done = set()
    prefix_rgx = re.compile('\s+(\w+):\w+\s+')
    decl = []
    for prefix in prefix_rgx.findall(query):
        if prefix in namespaces and prefix not in done:
            decl.append('PREFIX {}: <{}>'.format(prefix, namespaces[prefix]))
            done.add(prefix)
    if decl:
        query = '{}\n\n{}'.format('\n'.join(decl), query)
    return query


if __name__ == '__main__':
    database = SPARQLDatabase('http://data.bnf.fr/sparql')
    results = database.execute('''
PREFIX bnf-onto: <http://data.bnf.fr/ontology/bnf-onto/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?concept ?prop ?value WHERE {
 ?concept bnf-onto:FRBNF 14027233;
    ?prop ?value.

}
    ''')
    for r in results:
        print(r[0], r.concept)
    results = database.execute('SELECT * WHERE {?s ?p ?o} LIMIT 10')
    for r in results:
        print(r.s, r.p, r.o)
    db = DatabnfDatabase(cache_dir='__bnfjson_cache__')
    props = db.fetch_authority_infos(11917290)
    print([url for url in props['foaf:depiction'] if 'wikimedia' in url])
