from functools import reduce
import pandas as pd
import requests
import requests.adapters
import typing as t
import io
import urllib3.util

class Dataset(t.TypedDict):
    slug: str
    columns: t.Mapping[str, str]
#     transform: t.NotRequired[t.Callable[[pd.DataFrame], pd.DataFrame]]
    cite: str

DATASETS: t.Final[list[Dataset]] = [
    {
        'slug': 'gdp-per-capita-worldbank',
        'columns': {'ny_gdp_pcap_pp_kd': 'gdp_pcap_wb'},
        'cite': 'Eurostat, OECD, and World Bank (2025) – with minor processing by Our World in Data',
    },
    {
        'slug': 'gdp-worldbank',
        'columns': {'ny_gdp_mktp_pp_kd': 'gdp_wb'},
        'cite': 'Feenstra et al. - Penn World Table (2023) – with major processing by Our World in Data',
    },
    {
        'slug': 'democracy-index-eiu',
        'columns': {'democracy_eiu': 'democracy'},
        'cite': 'Economist Intelligence Unit (2006-2024) – processed by Our World in Data',
    },
    {
        'slug': 'human-rights-index-vdem',
        'columns': {'civ_libs_vdem__estimate_best': 'hdi'},
        'cite': 'V-Dem (2025) – processed by Our World in Data',
    },
    {
        'slug': 'country-position-nuclear-weapons',
        'columns': {'status': 'nuclear_weapons_position'},
        'cite': 'Bleek (2017); Nuclear Threat Initiative (2024) – with major processing by Our World in Data',
    },
    {
        'slug': 'military-spending-sipri',
        'columns': {'constant_usd': 'military_spending_sipri_usd_adjusted'},
        'cite': 'Stockholm International Peace Research Institute (2025) – with minor processing by Our World in Data.',
    },
    {
        'slug': 'armed-forces-personnel',
        'columns': {'ms_mil_totl_p1': 'armed_forces_personnel_iiss'},
    },
    {'slug': 'child-mortality-igme', 'columns': {
        'observation_value__indicator_child_mortality_rate__sex_total__wealth_quintile_total__unit_of_measure_deaths_per_100_live_births': 'child_mortality_rate'}},
    {'slug': 'infant-mortality', 'columns': {
        'observation_value__indicator_infant_mortality_rate__sex_total__wealth_quintile_total__unit_of_measure_deaths_per_100_live_births': 'infant_mortality_rate'}},
    {'slug': 'homicide-rate-unodc', 'columns': {
        'value__category_total__sex_total__age_total__unit_of_measurement_rate_per_100_000_population': 'homicide_rate'}},
    {'slug': 'share-of-population-urban', 'columns': {'sp_urb_totl_in_zs': 'urban_population_share'}},
    {'slug': 'share-of-population-in-extreme-poverty', 'columns': {
        'headcount_ratio__ppp_version_2021__poverty_line_300__welfare_type_income_or_consumption__table_income_or_consumption_consolidated__survey_comparability_no_spells': 'extreme_poverty_share'}},
    {'slug': 'political-corruption-index', 'columns': {'corruption_vdem__estimate_best': 'political_corruption_index'}},
    {'slug': 'rule-of-law-index', 'columns': {'rule_of_law_vdem__estimate_best': 'rule_of_law_index'}},
    {'slug': 'academic-freedom-index', 'columns': {'v2xca_academ__estimate_best': 'academic_freedom_index'}},
    {'slug': 'freedom-of-association-index',
     'columns': {'freeassoc_vdem__estimate_best': 'freedom_of_association_index'}},
    {'slug': 'freedom-of-expression-index', 'columns': {'freeexpr_vdem__estimate_best': 'freedom_of_expression_index'}},
    # {'slug': '', 'columns': {'': ''}},
]

def _session_default_make() -> requests.Session:
    session = requests.Session()
    retries = urllib3.util.Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[502, 503, 504],
    )
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))
    return session
_session_default: t.Final[requests.Session] = _session_default_make()

def _fetch(ds: Dataset, *, session: requests.Session = _session_default) -> pd.DataFrame:
    response = session.get(
        f"https://ourworldindata.org/grapher/{ds['slug']}.csv?v=1&csvType=full&useColumnShortNames=true",
        stream=True,
        timeout=60,
    )
    response.raise_for_status()
    csv = response.content.decode('utf-8')
    df = pd.read_csv(io.StringIO(csv))
    df = df[['Code','Year'] + list(ds['columns'])]
    df.dropna(subset=['Code','Year'] + list(ds['columns']), inplace=True)
    df.set_index(['Code','Year'], inplace=True)
    df.rename(columns=ds['columns'], inplace=True, errors='raise')
    # if 'transform' in ds: df = ds['transform'](df)
    return df

def _fetch_citation(ds: Dataset, *, session: requests.Session = _session_default) -> list[str]:
    columns = list(ds['columns'])
    response = session.get(f"https://ourworldindata.org/grapher/{ds['slug']}.metadata.json?v=1&csvType=full&useColumnShortNames=true")
    response.raise_for_status()
    metadata = response.json()
    citations = [
        metadata["columns"][column]['citationLong']
        for column in columns
        if column in metadata["columns"]
    ]
    return citations

def agg(*, session: requests.Session = _session_default) -> pd.DataFrame:
    dfs = map(lambda ds: _fetch(ds, session=session), DATASETS)
    df = reduce(lambda left, right: pd.merge(left,right,on=['Code','Year'],how='outer'), dfs)
    return df

def cite(
    *,
    session: requests.Session = _session_default,
) -> list[str]:
    xss = map(lambda ds: _fetch_citation(ds, session=session), DATASETS)
    return [x for xs in xss for x in xs]