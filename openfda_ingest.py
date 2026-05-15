#!/usr/bin/env python3
"""
RxCheck OpenFDA Ingestion Pipeline
====================================

Fetches drug interaction text from OpenFDA's drug labeling API and produces
a JSON file that can be loaded into rxcheck.html as a supplemental layer.

This pipeline pulls the `drug_interactions` field from FDA Structured Product
Labels (SPL) and converts the free-text interaction sections into a searchable
pair-based dataset.

USAGE
-----
1. (Optional) Get a free API key at https://open.fda.gov/apis/authentication/
   to raise rate limits from 240/min to 240/min per IP + 1000/day to 120k/day.
   Set environment variable: export OPENFDA_API_KEY=your_key_here
2. Edit DRUG_LIST below to include the generic names you want to ingest.
3. Run: python3 openfda_ingest.py
4. Output: openfda_interactions.json

REQUIREMENTS
------------
Python 3.8+ standard library only — no pip installs needed.

LIMITATIONS — READ THIS
------------------------
- OpenFDA interaction sections are free-text prose, not structured pairs.
- This pipeline detects interactions by searching for drug names mentioned
  in each label's drug_interactions field. False positives are possible
  (e.g., drug class mentions trigger matches). False negatives also
  happen (drug mentioned only by therapeutic class).
- No severity ratings are inferred. Severity is assigned heuristically
  ('label-mentioned') and should be treated as "review needed" rather
  than authoritative.
- Always cross-reference high-stakes pairs against primary references.
- Rate limit: 1000 requests/day without API key, 120000 with.
- This adds DEPTH but not CURATION. Keep the hand-curated dataset as
  the primary authoritative source.
"""

import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

API_BASE = "https://api.fda.gov/drug/label.json"
API_KEY = os.environ.get("OPENFDA_API_KEY", "")
RATE_LIMIT_DELAY = 0.3  # seconds between requests; raise if you get 429s
OUTPUT_FILE = "openfda_interactions.json"

# Generic drug names to ingest. Brand names are searched as a fallback.
# 625-drug list covering community pharmacy + specialty meds.
DRUG_LIST = [
    # ===== Anticoagulants / antiplatelets =====
    "warfarin", "apixaban", "rivaroxaban", "dabigatran", "edoxaban",
    "enoxaparin", "dalteparin", "tinzaparin", "fondaparinux", "heparin",
    "clopidogrel", "ticagrelor", "prasugrel", "aspirin", "dipyridamole",
    "cilostazol", "pentoxifylline",

    # ===== NSAIDs =====
    "ibuprofen", "naproxen", "diclofenac", "celecoxib", "ketorolac",
    "meloxicam", "indomethacin", "etodolac", "piroxicam", "nabumetone",
    "sulindac", "ketoprofen", "flurbiprofen", "tolmetin", "mefenamic acid",

    # ===== Antidepressants - SSRIs/SNRIs =====
    "sertraline", "fluoxetine", "paroxetine", "citalopram", "escitalopram",
    "fluvoxamine", "vilazodone", "vortioxetine",
    "venlafaxine", "duloxetine", "desvenlafaxine", "levomilnacipran",

    # ===== Other antidepressants =====
    "bupropion", "mirtazapine", "trazodone", "nefazodone",
    "amitriptyline", "nortriptyline", "imipramine", "desipramine", "doxepin",
    "clomipramine", "protriptyline", "amoxapine", "maprotiline",

    # ===== MAOIs =====
    "phenelzine", "tranylcypromine", "selegiline", "rasagiline", "linezolid",
    "isocarboxazid", "safinamide",

    # ===== Opioids =====
    "morphine", "oxycodone", "hydromorphone", "fentanyl", "codeine",
    "tramadol", "tapentadol", "methadone", "buprenorphine",
    "hydrocodone", "oxymorphone", "meperidine", "sufentanil", "remifentanil",
    "butorphanol", "nalbuphine", "pentazocine", "levorphanol",

    # ===== Benzos / Z-drugs / Sleep =====
    "lorazepam", "clonazepam", "alprazolam", "diazepam", "temazepam",
    "oxazepam", "midazolam", "triazolam", "chlordiazepoxide", "clobazam",
    "flurazepam", "estazolam", "clorazepate",
    "zopiclone", "zolpidem", "eszopiclone", "zaleplon",
    "suvorexant", "lemborexant", "ramelteon",

    # ===== Beta-blockers =====
    "metoprolol", "bisoprolol", "atenolol", "carvedilol", "propranolol",
    "labetalol", "nadolol", "nebivolol", "pindolol", "acebutolol",
    "esmolol", "sotalol", "timolol",

    # ===== Calcium channel blockers =====
    "amlodipine", "nifedipine", "felodipine", "nicardipine", "lercanidipine",
    "isradipine", "nimodipine",
    "diltiazem", "verapamil",

    # ===== ACE inhibitors =====
    "ramipril", "lisinopril", "perindopril", "enalapril", "captopril",
    "quinapril", "fosinopril", "trandolapril", "benazepril", "moexipril",

    # ===== ARBs =====
    "losartan", "valsartan", "candesartan", "telmisartan", "irbesartan",
    "olmesartan", "azilsartan", "eprosartan",

    # ===== ARNI / aliskiren =====
    "sacubitril and valsartan", "aliskiren",

    # ===== Other cardiovascular =====
    "digoxin", "amiodarone", "dronedarone", "flecainide", "propafenone",
    "ivabradine", "ranolazine", "mexiletine", "disopyramide",
    "clonidine", "methyldopa", "hydralazine", "minoxidil",
    "prazosin", "doxazosin", "terazosin", "tamsulosin", "silodosin",
    "alfuzosin",

    # ===== Diuretics =====
    "furosemide", "bumetanide", "torsemide", "ethacrynic acid",
    "hydrochlorothiazide", "chlorthalidone", "indapamide", "metolazone",
    "spironolactone", "eplerenone", "amiloride", "triamterene",
    "acetazolamide",

    # ===== Statins / lipid =====
    "atorvastatin", "rosuvastatin", "simvastatin", "pravastatin",
    "lovastatin", "fluvastatin", "pitavastatin",
    "ezetimibe", "gemfibrozil", "fenofibrate", "bezafibrate",
    "cholestyramine", "colesevelam", "colestipol",
    "evolocumab", "alirocumab", "icosapent ethyl", "niacin",
    "inclisiran", "bempedoic acid",

    # ===== Diabetes =====
    "metformin", "gliclazide", "glyburide", "glimepiride", "glipizide",
    "tolbutamide",
    "repaglinide", "nateglinide",
    "sitagliptin", "linagliptin", "saxagliptin", "alogliptin",
    "empagliflozin", "dapagliflozin", "canagliflozin", "ertugliflozin",
    "semaglutide", "liraglutide", "dulaglutide", "tirzepatide", "exenatide",
    "pioglitazone", "rosiglitazone",
    "acarbose", "miglitol",
    "pramlintide",
    "insulin glargine", "insulin lispro", "insulin aspart",
    "insulin detemir", "insulin degludec", "regular insulin",
    "insulin lispro protamine", "nph insulin",

    # ===== Antibiotics - penicillins =====
    "amoxicillin", "amoxicillin and clavulanate potassium", "ampicillin",
    "penicillin v potassium", "penicillin g", "dicloxacillin",
    "piperacillin and tazobactam",

    # ===== Antibiotics - cephalosporins =====
    "cephalexin", "cefadroxil", "cefazolin", "cefaclor", "cefuroxime",
    "cefprozil", "cefixime", "ceftriaxone", "cefdinir", "cefpodoxime",
    "cefepime", "ceftazidime", "ceftaroline",

    # ===== Antibiotics - macrolides =====
    "azithromycin", "clarithromycin", "erythromycin",

    # ===== Antibiotics - fluoroquinolones =====
    "ciprofloxacin", "levofloxacin", "moxifloxacin", "ofloxacin",
    "norfloxacin", "delafloxacin",

    # ===== Antibiotics - tetracyclines =====
    "doxycycline", "minocycline", "tetracycline", "tigecycline",
    "demeclocycline",

    # ===== Antibiotics - other =====
    "metronidazole", "tinidazole",
    "trimethoprim and sulfamethoxazole", "trimethoprim",
    "nitrofurantoin", "fosfomycin",
    "clindamycin", "lincomycin",
    "vancomycin", "daptomycin",
    "rifampin", "rifabutin", "rifaximin",
    "isoniazid", "ethambutol", "pyrazinamide",
    "gentamicin", "tobramycin", "amikacin", "neomycin",
    "meropenem", "imipenem and cilastatin", "ertapenem", "doripenem",
    "aztreonam",
    "polymyxin b", "colistimethate",
    "telavancin", "dalbavancin", "oritavancin",
    "fidaxomicin",
    "dapsone",

    # ===== Antifungals =====
    "fluconazole", "ketoconazole", "itraconazole", "voriconazole",
    "posaconazole", "isavuconazonium",
    "terbinafine", "griseofulvin",
    "caspofungin", "micafungin", "anidulafungin",
    "amphotericin b", "nystatin",
    "clotrimazole", "miconazole",

    # ===== Antivirals =====
    "acyclovir", "valacyclovir", "famciclovir",
    "oseltamivir", "zanamivir", "baloxavir marboxil",
    "ganciclovir", "valganciclovir", "foscarnet",
    "ribavirin", "sofosbuvir", "ledipasvir and sofosbuvir",
    "sofosbuvir and velpatasvir", "glecaprevir and pibrentasvir",
    "entecavir", "tenofovir disoproxil fumarate", "tenofovir alafenamide",
    "lamivudine", "emtricitabine", "abacavir",
    "efavirenz", "nevirapine", "rilpivirine", "doravirine", "etravirine",
    "dolutegravir", "bictegravir", "raltegravir", "elvitegravir",
    "darunavir", "atazanavir", "ritonavir", "cobicistat", "lopinavir",
    "maraviroc",
    "remdesivir",

    # ===== GI / acid suppression =====
    "omeprazole", "pantoprazole", "esomeprazole", "rabeprazole",
    "lansoprazole", "dexlansoprazole",
    "famotidine", "nizatidine", "cimetidine",
    "sucralfate", "misoprostol",
    "ondansetron", "granisetron", "palonosetron", "dolasetron",
    "metoclopramide", "prochlorperazine", "promethazine", "trimethobenzamide",
    "domperidone",
    "aprepitant", "fosaprepitant", "rolapitant",
    "loperamide", "diphenoxylate and atropine",
    "bismuth subsalicylate",
    "lubiprostone", "linaclotide", "plecanatide", "prucalopride",
    "polyethylene glycol", "lactulose", "bisacodyl", "senna", "docusate",
    "psyllium", "methylcellulose",
    "dicyclomine", "hyoscyamine",
    "mesalamine", "sulfasalazine", "balsalazide", "olsalazine",
    "ursodiol",

    # ===== Anticonvulsants =====
    "phenytoin", "fosphenytoin",
    "carbamazepine", "oxcarbazepine", "eslicarbazepine",
    "valproic acid", "divalproex sodium",
    "lamotrigine", "levetiracetam", "brivaracetam",
    "topiramate", "zonisamide",
    "gabapentin", "pregabalin",
    "phenobarbital", "primidone",
    "ethosuximide",
    "tiagabine", "vigabatrin",
    "lacosamide", "perampanel", "rufinamide", "felbamate",
    "cenobamate", "cannabidiol",

    # ===== Antipsychotics =====
    "quetiapine", "risperidone", "olanzapine", "aripiprazole", "paliperidone",
    "ziprasidone", "lurasidone", "asenapine", "iloperidone", "brexpiprazole",
    "cariprazine", "lumateperone", "pimavanserin",
    "clozapine",
    "haloperidol", "fluphenazine", "perphenazine", "trifluoperazine",
    "chlorpromazine", "thioridazine", "thiothixene", "loxapine",

    # ===== Migraine / triptans =====
    "sumatriptan", "rizatriptan", "zolmitriptan", "eletriptan", "naratriptan",
    "almotriptan", "frovatriptan",
    "ergotamine", "dihydroergotamine",
    "erenumab", "fremanezumab", "galcanezumab", "eptinezumab",
    "rimegepant", "ubrogepant", "atogepant", "lasmiditan",

    # ===== Erectile dysfunction =====
    "sildenafil", "tadalafil", "vardenafil", "avanafil",

    # ===== BPH / Urology =====
    "tamsulosin", "doxazosin", "terazosin", "alfuzosin", "silodosin",
    "finasteride", "dutasteride",
    "oxybutynin", "tolterodine", "solifenacin", "darifenacin", "trospium",
    "fesoterodine", "mirabegron", "vibegron",

    # ===== Thyroid =====
    "levothyroxine", "liothyronine", "thyroid",
    "methimazole", "propylthiouracil",

    # ===== Corticosteroids =====
    "prednisone", "prednisolone", "methylprednisolone", "dexamethasone",
    "hydrocortisone", "triamcinolone", "fludrocortisone", "budesonide",
    "betamethasone", "deflazacort",

    # ===== Rheumatology / DMARDs =====
    "methotrexate", "leflunomide", "sulfasalazine", "hydroxychloroquine",
    "tofacitinib", "baricitinib", "upadacitinib", "filgotinib",
    "apremilast",
    "auranofin",
    "penicillamine",
    "azathioprine", "mycophenolate", "cyclosporine", "tacrolimus",
    "sirolimus", "everolimus",
    "cyclophosphamide",

    # ===== Biologics =====
    "adalimumab", "etanercept", "infliximab", "certolizumab pegol",
    "golimumab",
    "rituximab", "tocilizumab", "sarilumab",
    "ustekinumab", "secukinumab", "ixekizumab", "brodalumab",
    "risankizumab", "guselkumab", "tildrakizumab",
    "abatacept", "anakinra", "canakinumab", "belimumab",
    "omalizumab", "dupilumab", "mepolizumab", "benralizumab", "reslizumab",
    "vedolizumab", "natalizumab", "ocrelizumab", "ofatumumab",

    # ===== Gout =====
    "allopurinol", "febuxostat", "colchicine", "probenecid", "rasburicase",
    "pegloticase",

    # ===== Mood stabilizers =====
    "lithium",

    # ===== Oncology - oral =====
    "tamoxifen", "anastrozole", "letrozole", "exemestane",
    "fulvestrant",
    "bicalutamide", "flutamide", "nilutamide", "abiraterone", "enzalutamide",
    "apalutamide", "darolutamide",
    "leuprolide", "goserelin", "triptorelin", "degarelix",
    "imatinib", "dasatinib", "nilotinib", "bosutinib", "ponatinib",
    "erlotinib", "gefitinib", "afatinib", "osimertinib",
    "sunitinib", "sorafenib", "pazopanib", "regorafenib", "cabozantinib",
    "lenvatinib", "vandetanib",
    "lapatinib", "neratinib",
    "ibrutinib", "acalabrutinib", "zanubrutinib",
    "palbociclib", "ribociclib", "abemaciclib",
    "venetoclax",
    "olaparib", "rucaparib", "niraparib", "talazoparib",
    "everolimus", "temsirolimus",
    "capecitabine",
    "lenalidomide", "pomalidomide", "thalidomide",
    "ruxolitinib", "fedratinib",
    "vismodegib", "sonidegib",
    "vemurafenib", "dabrafenib", "encorafenib",
    "trametinib", "cobimetinib", "binimetinib",
    "selpercatinib", "pralsetinib",

    # ===== Respiratory =====
    "salbutamol", "albuterol", "levalbuterol", "terbutaline",
    "salmeterol", "formoterol", "arformoterol", "indacaterol", "olodaterol",
    "ipratropium", "tiotropium", "aclidinium", "umeclidinium",
    "glycopyrronium",
    "fluticasone propionate", "fluticasone furoate", "budesonide",
    "mometasone", "beclomethasone", "ciclesonide",
    "montelukast", "zafirlukast", "zileuton",
    "theophylline", "aminophylline",
    "roflumilast",
    "ivacaftor", "lumacaftor and ivacaftor", "tezacaftor",
    "elexacaftor", "dornase alfa",
    "nintedanib", "pirfenidone",

    # ===== Anti-allergy / antihistamines =====
    "diphenhydramine", "hydroxyzine", "chlorpheniramine", "doxylamine",
    "cetirizine", "levocetirizine", "loratadine", "desloratadine",
    "fexofenadine",
    "cromolyn",

    # ===== ADHD / stimulants =====
    "methylphenidate", "dexmethylphenidate",
    "amphetamine", "dextroamphetamine", "lisdexamfetamine",
    "atomoxetine", "guanfacine", "clonidine",

    # ===== Substance use =====
    "naltrexone", "naloxone", "naloxegol", "methylnaltrexone",
    "varenicline",
    "disulfiram", "acamprosate",
    "nicotine",

    # ===== Bone / osteoporosis =====
    "alendronate", "risedronate", "ibandronate", "zoledronic acid",
    "denosumab", "teriparatide", "abaloparatide", "romosozumab",
    "raloxifene", "bazedoxifene",
    "calcitonin",

    # ===== Hormonal contraception / HRT =====
    "ethinyl estradiol", "norethindrone", "norgestimate", "levonorgestrel",
    "etonogestrel", "drospirenone", "desogestrel",
    "estradiol", "conjugated estrogens", "estropipate",
    "medroxyprogesterone", "progesterone", "norethindrone acetate",
    "ulipristal", "mifepristone",
    "testosterone",

    # ===== Vitamins / supplements =====
    "folic acid", "cyanocobalamin", "thiamine", "pyridoxine",
    "ergocalciferol", "cholecalciferol", "calcitriol", "paricalcitol",
    "ferrous sulfate", "ferrous gluconate", "ferrous fumarate",
    "ferric carboxymaltose", "iron sucrose",
    "potassium chloride", "magnesium oxide", "calcium carbonate",
    "calcium citrate",

    # ===== Misc =====
    "acetaminophen",
    "memantine", "donepezil", "rivastigmine", "galantamine",
    "ropinirole", "pramipexole", "rotigotine", "carbidopa and levodopa",
    "entacapone", "tolcapone", "amantadine", "benztropine", "trihexyphenidyl",
    "tetrabenazine", "deutetrabenazine", "valbenazine",
    "interferon beta-1a", "interferon beta-1b", "glatiramer",
    "dimethyl fumarate", "diroximel fumarate", "fingolimod", "siponimod",
    "ozanimod", "ponesimod", "teriflunomide", "cladribine",
    "edaravone", "riluzole",
    "octreotide", "lanreotide",
    "pancrelipase",
    "ursodiol",
    "tolvaptan", "conivaptan",
    "sevelamer", "lanthanum carbonate", "calcium acetate",
    "patiromer", "sodium polystyrene sulfonate", "sodium zirconium cyclosilicate",
    "cinacalcet", "etelcalcetide",
    "modafinil", "armodafinil", "pitolisant", "solriamfetol",
    "epoetin alfa", "darbepoetin alfa",
    "filgrastim", "pegfilgrastim", "sargramostim",
    "eltrombopag", "romiplostim", "avatrombopag",
    "isotretinoin", "acitretin",
    "tretinoin", "adapalene", "tazarotene",
    "benzoyl peroxide", "clindamycin phosphate", "dapsone",
    "spironolactone",

    # ===== Ophthalmics with systemic absorption =====
    "latanoprost", "bimatoprost", "travoprost", "tafluprost",
    "brimonidine", "apraclonidine",
    "dorzolamide", "brinzolamide",
    "pilocarpine",

    # ===== Pulmonary HTN =====
    "bosentan", "ambrisentan", "macitentan",
    "riociguat",
    "epoprostenol", "iloprost", "treprostinil", "selexipag",

    # ===== Herbals / OTC commonly missed =====
    "st johns wort",
    "melatonin",
    "loperamide", "simethicone",

    # ===== Heart failure newer =====
    "sacubitril and valsartan",
    "dapagliflozin", "empagliflozin",
    "vericiguat",
    "mavacamten",

    # ===== Antifibrotic / specialty =====
    "tafamidis",
    "patisiran", "inotersen",

    # ===== Sickle cell / anemia =====
    "hydroxyurea", "voxelotor", "crizanlizumab",
    "luspatercept",

    # ===== Misc newer =====
    "rivaroxaban", "betrixaban",
    "selumetinib",
    "trifluridine and tipiracil",
    "vortioxetine",
    "esketamine",
]

# Deduplicate while preserving order
DRUG_LIST = list(dict.fromkeys(DRUG_LIST))



# ============================================================================
# API CLIENT
# ============================================================================

def fetch_label(generic_name):
    """Fetch the most recent label for a generic drug name from OpenFDA."""
    query = f'openfda.generic_name:"{generic_name}"'
    params = {
        "search": query,
        "limit": 1,
        "sort": "effective_time:desc",
    }
    if API_KEY:
        params["api_key"] = API_KEY

    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"

    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.load(resp)
            if "results" in data and data["results"]:
                return data["results"][0]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # No label found for this generic name — try without quotes
            return fetch_label_loose(generic_name)
        elif e.code == 429:
            print(f"  [rate limit hit, sleeping 30s]")
            time.sleep(30)
            return fetch_label(generic_name)
        else:
            print(f"  [HTTP {e.code} for {generic_name}]")
    except Exception as e:
        print(f"  [error fetching {generic_name}: {e}]")
    return None


def fetch_label_loose(generic_name):
    """Fallback search without exact quotes."""
    query = f"openfda.generic_name:{generic_name.split()[0]}"
    params = {"search": query, "limit": 1}
    if API_KEY:
        params["api_key"] = API_KEY
    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.load(resp)
            if "results" in data and data["results"]:
                return data["results"][0]
    except Exception:
        pass
    return None


# ============================================================================
# PARSING
# ============================================================================

def extract_interaction_text(label):
    """Get the drug_interactions field as plain text."""
    if not label:
        return ""
    sections = label.get("drug_interactions", [])
    if not sections:
        return ""
    return "\n".join(sections)


def detect_mentioned_drugs(text, drug_list, source_drug):
    """
    Find which drugs from drug_list are mentioned in the interaction text
    of source_drug. Returns a set of generic names found.
    """
    if not text:
        return set()
    text_lower = text.lower()
    found = set()
    for d in drug_list:
        if d == source_drug:
            continue
        # Match the first word of the generic name (handles "trimethoprim and sulfamethoxazole")
        first_word = d.split()[0].lower()
        # Word boundary match
        pattern = r'\b' + re.escape(first_word) + r'\b'
        if re.search(pattern, text_lower):
            found.add(d)
    return found


def extract_severity_hint(text, target_drug):
    """
    Heuristically infer severity from how target_drug is described in text.
    Returns: 'contraindicated', 'major', 'moderate', or 'unknown'.
    """
    if not text or not target_drug:
        return "unknown"
    text_lower = text.lower()
    target_lower = target_drug.lower().split()[0]

    # Find context window (200 chars before/after the drug mention)
    idx = text_lower.find(target_lower)
    if idx == -1:
        return "unknown"
    start = max(0, idx - 200)
    end = min(len(text_lower), idx + 200)
    context = text_lower[start:end]

    # Severity keyword heuristics — must appear near the drug name
    if any(kw in context for kw in [
        "contraindicated", "do not use", "avoid concomitant",
        "should not be used", "do not coadminister"
    ]):
        return "contraindicated"
    if any(kw in context for kw in [
        "avoid", "not recommended", "fatal", "life-threatening",
        "severe", "increased risk of bleeding", "rhabdomyolysis",
        "hypertensive crisis", "serotonin syndrome", "torsades",
        "respiratory depression"
    ]):
        return "major"
    if any(kw in context for kw in [
        "monitor", "consider dose reduction", "use with caution",
        "may increase", "may decrease", "adjust dose"
    ]):
        return "moderate"
    return "unknown"


# ============================================================================
# PIPELINE
# ============================================================================

def extract_drug_info(label):
    """Extract drug info fields from FDA label: class, MOA, indications, adverse reactions."""
    if not label:
        return None

    def get_field(field_name):
        """Helper to get a field as joined string."""
        val = label.get(field_name, [])
        if isinstance(val, list):
            return " ".join(val) if val else ""
        return str(val) if val else ""

    # openfda subfield has structured class info
    openfda = label.get("openfda", {})
    pharm_class = []
    for f in ["pharm_class_epc", "pharm_class_moa", "pharm_class_cs", "pharm_class_pe"]:
        v = openfda.get(f, [])
        if v:
            pharm_class.extend(v)

    return {
        "generic_name": (openfda.get("generic_name", []) or [""])[0],
        "brand_names": openfda.get("brand_name", []),
        "pharm_class": pharm_class,
        "indications": get_field("indications_and_usage"),
        "mechanism_of_action": get_field("mechanism_of_action"),
        "clinical_pharmacology": get_field("clinical_pharmacology"),
        "adverse_reactions": get_field("adverse_reactions"),
        "warnings": get_field("warnings_and_cautions") or get_field("warnings"),
        "contraindications": get_field("contraindications"),
        "boxed_warning": get_field("boxed_warning"),
    }


def run():
    print(f"OpenFDA ingestion pipeline")
    print(f"==========================")
    print(f"API key: {'YES (high rate limit)' if API_KEY else 'NO (1000/day limit)'}")
    print(f"Drugs to fetch: {len(DRUG_LIST)}")
    print()

    raw_labels = {}        # interaction text per drug
    drug_info = {}         # full drug info per drug
    failed = []

    # Phase 1: Fetch all labels
    for i, drug in enumerate(DRUG_LIST, 1):
        print(f"[{i:3d}/{len(DRUG_LIST)}] Fetching: {drug}")
        label = fetch_label(drug)
        if label:
            text = extract_interaction_text(label)
            info = extract_drug_info(label)
            if info:
                drug_info[drug] = info
            if text:
                raw_labels[drug] = text
                print(f"          ✓ {len(text)} chars of interaction text + info")
            else:
                print(f"          - no interaction section (info still saved)")
                failed.append(drug)
        else:
            print(f"          ✗ no label found")
            failed.append(drug)
        time.sleep(RATE_LIMIT_DELAY)

    print(f"\nPhase 1 complete: {len(raw_labels)} labels w/ interactions, {len(drug_info)} w/ info")
    if failed:
        print(f"Failed/empty: {', '.join(failed[:20])}{'...' if len(failed) > 20 else ''}")

    # Phase 2: Build pair-based interaction map
    print(f"\nPhase 2: Detecting drug pairs from interaction text...")
    pairs = defaultdict(set)  # frozenset({a, b}) -> set of severity hints

    for source_drug, text in raw_labels.items():
        mentioned = detect_mentioned_drugs(text, list(raw_labels.keys()), source_drug)
        for target in mentioned:
            key = frozenset({source_drug, target})
            severity = extract_severity_hint(text, target)
            pairs[key].add(severity)

    # Phase 3: Build interactions output
    print(f"\nPhase 3: Building interactions output...")
    interactions = []
    for pair_set, severities in pairs.items():
        a, b = sorted(pair_set)
        priority = ["contraindicated", "major", "moderate", "unknown"]
        sev = next((s for s in priority if s in severities), "unknown")
        interactions.append({
            "drugA": a,
            "drugB": b,
            "severity_hint": sev,
            "source": "openfda_label",
            "snippet_a": _snippet(raw_labels.get(a, ""), b),
            "snippet_b": _snippet(raw_labels.get(b, ""), a),
        })

    ix_output = {
        "generated": datetime.utcnow().isoformat() + "Z",
        "source": "OpenFDA drug label API",
        "drugs_fetched": len(raw_labels),
        "drugs_failed": len(failed),
        "interaction_count": len(interactions),
        "interactions": interactions,
        "disclaimer": (
            "Severity hints are heuristically inferred from label keywords "
            "and should be reviewed. This data supplements but does not "
            "replace curated interaction databases."
        )
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(ix_output, f, indent=2)

    # Phase 4: Build drug info output (separate file)
    print(f"\nPhase 4: Building drug info output...")
    info_output = {
        "generated": datetime.utcnow().isoformat() + "Z",
        "source": "OpenFDA drug label API",
        "drug_count": len(drug_info),
        "drugs": drug_info,
        "disclaimer": (
            "Information sourced from FDA Structured Product Labels. "
            "Adverse reactions and warnings may not be exhaustive."
        )
    }

    info_file = OUTPUT_FILE.replace("interactions", "druginfo")
    if info_file == OUTPUT_FILE:
        info_file = "openfda_druginfo.json"
    with open(info_file, "w") as f:
        json.dump(info_output, f, indent=2)

    print(f"\n✓ Done.")
    print(f"  Wrote {len(interactions)} interaction pairs to {OUTPUT_FILE}")
    print(f"  Wrote {len(drug_info)} drug info entries to {info_file}")
    print(f"\n  Severity breakdown:")
    sev_counts = defaultdict(int)
    for ix in interactions:
        sev_counts[ix["severity_hint"]] += 1
    for sev in ["contraindicated", "major", "moderate", "unknown"]:
        print(f"    {sev}: {sev_counts[sev]}")


def _snippet(text, drug, window=150):
    """Extract a snippet of text around the mention of drug."""
    if not text or not drug:
        return ""
    text_lower = text.lower()
    target = drug.lower().split()[0]
    idx = text_lower.find(target)
    if idx == -1:
        return ""
    start = max(0, idx - window)
    end = min(len(text), idx + window)
    snippet = text[start:end].strip()
    # Clean up whitespace
    snippet = re.sub(r"\s+", " ", snippet)
    return ("..." if start > 0 else "") + snippet + ("..." if end < len(text) else "")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\n\nInterrupted.")
        sys.exit(1)
