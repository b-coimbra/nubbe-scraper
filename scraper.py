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
    doc = None

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

        self.doc = BeautifulSoup(xml, 'lxml')

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
            'Location'           : 'cidade'
        }

        multiple_mappings = {
            'References' : 'compilado',
            'Species'    : 'origem'
        }

        for key in mappings:
            details[key] = self.__find(mappings[key])

        for key in multiple_mappings:
            details[key] = ', '.join(self.__find_all(multiple_mappings[key]))

        details['Biological Properties'] = self.__get_bio_properties()
        details['Source(s)'] = self.__get_sources()

        return details

    def __get_bio_properties(self) -> str:
        ids = self.__find_all('which')
        mappings = {
            '28': 'Activation of Glucoamylase',
            '55': 'Anesthetic',
            '18': 'Anthelmintic',
            '48': 'Antiallergenic',
            '22': 'Antiangiogenic',
            '13': 'Antibacterial',
            '1':  'Anticancer',
            '36': 'Antichagasic',
            '2':  'Antifungal',
            '15': 'Anti-inflamatory',
            '20': 'Antileishmanial',
            '11': 'Antimalarial',
            '14': 'Antinociceptive',
            '5':  'Antioxidant',
            '7':  'Antitrypanosomal',
            '54': 'Anti-ulcerogenic',
            '6':  'Antiviral',
            '23': 'Anxiolytic',
            '35': 'Cell growth inhbition',
            '9':  'Cytotoxic',
            '61': 'Genotoxic',
            '56': 'Induction of ROS production',
            '3':  'Inhibition of Acetylcholinesterase',
            '30': 'Inhibition of ATP synthesis',
            '31': 'Inhibition of basal electron transport',
            '47': 'Inhibition of Cathepsin B',
            '46': 'Inhibition of Cathepsin L',
            '40': 'Inhibition of Cathepsin V',
            '59': 'Inhibition of Cyclin-dependant Kinase (CDK)',
            '29': 'Inhibition of Glyceraldehyde-3- phosphate',
            '19': 'Inhibition of Glycosidase',
            '4':  'Inhibition of Heme polymerisation',
            '26': 'Inhibition of Myeloperoxidase',
            '25': 'Inhibition of NADPH Oxidase',
            '21': 'Inhibition of NF KB activation',
            '32': 'Inhibition of phosphorylating electron transport',
            '12': 'Inhibition of Protease',
            '24': 'Inhibition of ROS Production by Neutrophils',
            '58': 'Inhibition of Topoisomerase',
            '57': 'Inhibition of Tubulin polymerization',
            '33': 'Inhibition of uncoupled electron transport',
            '49': 'Insect antennae response',
            '8':  'Insecticidal',
            '50': 'Molluscicidal',
            '60': 'Mutagenic',
            '17': 'Nematostatic'
        }

        return ', '.join(mappings[i] for i in ids)

    def __get_sources(self) -> str:
        mappings = {
            '2': 'Semisynthesis',
            '3': 'Biotransformation product',
            '4': 'Isolated from a plant',
            '5': 'Isolated from a microorganism',
            '6': 'Isolated from a marine organism',
            '7': 'Isolated from animalia'
        }

        return mappings[self.__find('tipo')]

    def __find(self, tag) -> str:
        tags = self.__find_all(tag)

        if not tags: return ''

        return tags[0]

    def __find_all(self, tag) -> List[str]:
        tags = self.doc.find_all(tag)

        if len(tags) == 0: return ''

        return list(map(lambda x: x.text.strip().replace('\n', ' '), tags))


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
        details = [self.extract_molecule_detail(mol_id, idx, len(ids))
                   for idx, mol_id in enumerate(ids)]

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
