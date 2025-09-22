import importlib.resources as pkg_resources
import requests
import json
from astroquery.mpc import MPC
from datetime import datetime, timedelta
import pandas as pd
import polars as pl

class Information:
    """
    Clase que busca información general sobre objetos menores del sistema solar.
    """
    def __init__(self, selected_object: str):
        self.selected_object = selected_object
        self.families = None
        self._load_families()
        self.identifier = None  
        self._fetch_identifier()
        self.orbit_data = None
        self._fetch_orbit_data()
        
    # método privado para cargar diccionario con todas las familias
    def _load_families(self):
        # Abre family.json directamente desde el paquete, sin importar dónde esté instalado
        with pkg_resources.files(__package__).joinpath("family.json").open("r", encoding="utf-8") as f:
            self.families = json.load(f)
            
    #método privado que hace la consulta de la información del MPC
    def _fetch_identifier(self):
        url = "https://data.minorplanetcenter.net/api/query-identifier"
        try:
            response = requests.get(url,  data=self.selected_object)
            response.raise_for_status()
            self.identifier = response.json()
        except Exception as e:
            self.identifier = {"found": 0}  # fallback si hay error

        #Para objetos con un identificador igual, se toma el primero
        disambiguation_list = self.identifier.get("disambiguation_list")
        if disambiguation_list:
            response = requests.get(url,  data=disambiguation_list[0]['permid'])
            response.raise_for_status()
            self.identifier = response.json()     

    #--------------Buscar si el objeto esta en la base de datos del MPC------------------------
    def object_exists(self):
        #retorna True si sí esta en la base de datos y False si no esta
        if self.identifier.get("found") == 1:
            return True
        elif self.identifier.get("found") == 0:
            return False

    #-----------------ID ofical del MPC------------------------
    def ID_object(self):
        if self.identifier.get("found") == 1:
            return self.identifier.get('permid')
        elif self.identifier.get("found") == 0:
            return None

    #----------------Nombre del objeto----------------
    def name_object(self):
        if self.identifier.get("found") == 1:
            return self.identifier.get('name')
        elif self.identifier.get("found") == 0:
            return None
    #----------------Provisional designacion---------------------
    def provisional_designation(self):
        if self.identifier.get("found") == 1:
            return self.identifier.get('unpacked_primary_provisional_designation')
        elif self.identifier.get("found") == 0:
            return None
            
    #----------------Tipo de objeto------------------
    def object_type(self):
        if self.identifier.get("found") == 1:
            object_type = self.identifier['object_type'][0]
            if object_type == 'Comet' or object_type == 'Comet (Fragment)':
                return 'Cometa'
            elif object_type == 'Interstellar':
                return 'Objeto Interestelar'
            elif object_type=='Minor Planet' or object_type=='Minor Planet (Binary)':
                return 'Asteroide'
            elif object_type=='Natural Satellite (of planet)':
                return 'Satelite Natural'
            else:
                return 'Objeto'            
        elif self.identifier.get("found") == 0:
            return None
    
    #------------------familia----------------------
    def family_object(self):
        if self.ID_object() in self.families.keys():
            return self.families[self.ID_object()]
        else:
            return None
    #----------------------
    #
    #método privado que hace la consulta de la información del MPC
    def _fetch_orbit_data(self):
        if self.object_exists():
            if self.object_type() == 'Cometa':
                if self.ID_object() !=None:
                    self.orbit_data = MPC.query_object('comet',designation=self.ID_object())[0]
                else:
                    self.orbit_data = MPC.query_object('comet',designation=self.provisional_designation())[0]
                    
            elif self.object_type() == 'Asteroide':
                if self.ID_object() !=None:
                    self.orbit_data = MPC.query_object('asteroid',number=self.ID_object())[0]
                else:
                    self.orbit_data = MPC.query_object('asteroid',designation=self.provisional_designation())[0]
                    
            elif self.object_type() == 'Objeto Interestelar':
                url_COBS = f'https://cobs.si/api/comet.api?des={self.ID_object()}'
                response = requests.get(url_COBS)
                self.orbit_data = response.json()['object']  
            else:
                self.orbit_data = None
        else:
            self.orbit_data = None

    #---------------Periodo orbital----------------
    def orbital_period(self):
        if self.object_exists():
            if self.object_type() == 'Cometa' or self.object_type() == 'Asteroide':
                return self.orbit_data.get('period')
            else:
                return None
        else:
            return None

    #---------------Fecha perihelio--------------
    def date_perihelion(self):
        if self.object_exists():
            if self.object_type() == 'Cometa' or self.object_type() == 'Asteroide':
                base, frac = self.orbit_data.get('perihelion_date').split(".")           # Separa fecha base y fracción
                frac_day = float("0." + frac)         # Convierte la fracción a decimal
                base_date = datetime.strptime(base, "%Y-%m-%d")  # Convierte fecha base
                t = base_date + timedelta(days=frac_day) 
                return pd.to_datetime(t).tz_localize("UTC")     # Suma la fracción de día
            elif self.object_type() == 'Objeto Interestelar':
                return pd.to_datetime(self.orbit_data.get('perihelion_date'), utc=True)
            else:
                return None
        else:
            return None

    #----------------Existencia en COBS------------------
    def comet_exists_in_COBS(self,selected_object):
        url_list_comets = 'https://cobs.si/api/comet_list.api'
        response = requests.get(url_list_comets)
        if response.status_code == 200:
            content = response.json() 
            list_comets = pl.DataFrame(content['objects'])['name']
            if selected_object in list_comets:
                return True
            else:
                return False