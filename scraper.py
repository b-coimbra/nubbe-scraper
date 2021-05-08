#!/usr/bin/env python

import csv
import json
import asyncio
import requests

from bs4     import BeautifulSoup
from abc     import ABC, abstractmethod
from random  import random
from typing  import List

class Config:
    URL = "https://nubbe.iq.unesp.br/portal"
    OUTPUT_PATH = './molecule_structures.csv'


class Utils:
    @staticmethod
    def make_url(path):
        return f'{Config.URL}{path}'


class Request(ABC):
    def post(self): pass

    def get(self, id): pass

    @abstractmethod
    def get_url(self):
        raise NotImplementedError('No url was set.')


class MoleculeRequest(Request):
    SERVICE_TYPE = 17

    def __init__(self):
        self.path = '/do/Query'

    def get_url(self):
        return Utils.make_url(self.path)

    def post(self):
        return requests.post(self.get_url(), headers={
            'accept'          : '*/*',
            'accept-language' : 'en-US,en;q=0.9',
            'content-type'    : 'application/x-www-form-urlencoded',
            'sec-fetch-dest'  : 'empty',
            'sec-fetch-mode'  : 'cors',
            'sec-fetch-site'  : 'same-origin',
            'x-requested-with': 'XMLHttpRequest'
        }, data={
            'service': self.SERVICE_TYPE,
            'tipo_1': ''
        })


class MoleculeDetailRequest(Request):
    SERVICE_TYPE = 21

    def __init__(self):
        self.base_path = f'/do/Query?reqid={random()}&service={self.SERVICE_TYPE}'
        self.path = ''

    def get_url(self):
        return Utils.make_url(self.path)

    def get(self, molecule_id):
        self.path = f'{self.base_path}&id={molecule_id}'

        return requests.get(self.get_url(), headers={
            'accept'          : '*/*',
            'accept-language' : 'en-US,en;q=0.9',
            'sec-fetch-dest'  : 'empty',
            'sec-fetch-mode'  : 'cors',
            'sec-fetch-site'  : 'same-origin',
        })


class Parser:
    @abstractmethod
    def parse(self, data) -> dict:
        raise NotImplementedError('No parser was implemented.')


class MoleculesXmlParser(Parser):
    def parse(self, xml) -> dict:
        """ Retrieves the list of molecule IDs """

        xmldoc = BeautifulSoup(xml, 'lxml')
        itemlist = xmldoc.find_all('id')

        return { 'ids': [int(x.text) for x in itemlist] }


class MoleculeDetailXmlParser(Parser):
    def parse(self, xml) -> dict:
        """ Retrieves the info of a particular molecule """

        xmldoc = BeautifulSoup(xml, 'lxml')
        details  = {}
        mappings = {
            'NuBBE'              : 'cod',
            'Common Name'        : 'nome',
            'Inchi'              : 'inchi',
            'Inchikey'           : 'inchikey',
            'Chemical Class'     : 'classe',
            'Mol Formula'        : 'formol',
            'SMILES'             : 'smiles',
            'Molecula Mass'      : 'massa_molar',
            'Monoisotropic Mass' : 'massa_monoisotopica',
            'cLogP'              : 'logp',
            'TPSA'               : 'tpsa',
            'Lipinski Violations': 'nvlr',
            'H-bond acceptors'   : 'non',
            'H-bond donors'      : 'nohnh',
            'Rotatable Bonds'    : 'nrotb',
            'Molecular Volume'   : 'mol_vol',
            'References'         : 'compilado'
        }
        origin_mappings = ['familia', 'genero', 'especie']

        for key in mappings:
            details[key] = self.__get_tag_value(xmldoc, mappings[key])

        details['Species'] = ' '.join(self.__get_tag_value(xmldoc, origin) for origin in origin_mappings)

        return details

    def __get_tag_value(self, doc, tag_name):
        elems = doc.find_all(tag_name)

        if len(elems) == 0: return ''

        return elems[0].text


class FileExporter:
    @abstractmethod
    def exportTo(self, data, location):
        raise NotImplementedError('No export option for this format.')


class CsvFileExporter(FileExporter):
    def exportTo(self, data, location):
        with open(location, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)


class Progress:
    @staticmethod
    def show(current, total):
        print(f"[{current + 1}/{total}] Extracting data ...")


class Scraper:
    def __init__(self,
                 molecule_request: Request, molecule_detail_request: Request,
                 molecules_parser: Parser, molecule_detail_parser: Parser,
                 exporter: FileExporter):
        self.molecule_req = molecule_request
        self.molecule_detail_req = molecule_detail_request
        self.molecules_parser = molecules_parser
        self.molecule_detail_parser = molecule_detail_parser
        self.exporter = exporter

    async def scrape(self):
        ids = self.extract_molecule_ids()
        details = [self.extract_molecule_detail(mol_id, idx, len(ids)) for idx, mol_id in enumerate(ids)]

        self.exporter.exportTo(details, Config.OUTPUT_PATH)

    def extract_molecule_ids(self) -> List[int]:
        response = self.molecule_req.post()
        content = response.content

        return self.molecules_parser.parse(content)['ids']

    def extract_molecule_detail(self, id, current_item, total_items) -> dict:
        Progress.show(current_item, total_items)

        response = self.molecule_detail_req.get(id)
        content = response.content

        return self.molecule_detail_parser.parse(content)


async def main():
    scraper = Scraper(
        MoleculeRequest(), MoleculeDetailRequest(),
        MoleculesXmlParser(), MoleculeDetailXmlParser(),
        CsvFileExporter()
    )
    await scraper.scrape()


if __name__ == '__main__':
    asyncio.run(main())
