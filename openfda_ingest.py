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
# Start with this list and expand to match your dispensing volume.
DRUG_LIST = [
    # Anticoagulants / antiplatelets
    "warfarin", "apixaban", "rivaroxaban", "dabigatran", "edoxaban",
    "enoxaparin", "heparin", "clopidogrel", "ticagrelor", "prasugrel",
    "aspirin", "dipyridamole",

    # NSAIDs
    "ibuprofen", "naproxen", "diclofenac", "celecoxib", "ketorolac",
    "meloxicam", "indomethacin",

    # Antidepressants
    "sertraline", "fluoxetine", "paroxetine", "citalopram", "escitalopram",
    "fluvoxamine", "venlafaxine", "duloxetine", "desvenlafaxine",
    "bupropion", "mirtazapine", "trazodone", "amitriptyline", "nortriptyline",

    # MAOIs
    "phenelzine", "tranylcypromine", "selegiline", "rasagiline", "linezolid",

    # Opioids
    "morphine", "oxycodone", "hydromorphone", "fentanyl", "codeine",
    "tramadol", "tapentadol", "methadone", "buprenorphine",

    # Benzos / Z-drugs
    "lorazepam", "clonazepam", "alprazolam", "diazepam", "temazepam",
    "zopiclone", "zolpidem",

    # Cardiovascular
    "metoprolol", "bisoprolol", "atenolol", "carvedilol", "propranolol",
    "labetalol", "amlodipine", "nifedipine", "diltiazem", "verapamil",
    "ramipril", "lisinopril", "perindopril", "enalapril",
    "losartan", "valsartan", "candesartan", "telmisartan", "irbesartan",
    "digoxin", "amiodarone", "sotalol", "dronedarone",
    "furosemide", "hydrochlorothiazide", "spironolactone", "eplerenone",

    # Statins
    "atorvastatin", "rosuvastatin", "simvastatin", "pravastatin",
    "lovastatin", "ezetimibe", "gemfibrozil", "fenofibrate",

    # Diabetes
    "metformin", "gliclazide", "glyburide", "glimepiride",
    "sitagliptin", "linagliptin",
    "empagliflozin", "dapagliflozin", "canagliflozin",
    "semaglutide", "liraglutide", "dulaglutide", "tirzepatide",
    "insulin glargine", "insulin lispro", "insulin aspart",

    # Antibiotics
    "amoxicillin", "azithromycin", "clarithromycin", "erythromycin",
    "ciprofloxacin", "levofloxacin", "moxifloxacin",
    "doxycycline", "minocycline", "metronidazole",
    "trimethoprim and sulfamethoxazole", "nitrofurantoin",
    "cephalexin", "cefuroxime", "ceftriaxone",
    "clindamycin", "vancomycin", "rifampin",
    "fluconazole", "ketoconazole", "itraconazole", "voriconazole",
    "acyclovir", "valacyclovir", "oseltamivir",

    # GI / PPIs
    "omeprazole", "pantoprazole", "esomeprazole", "rabeprazole",
    "lansoprazole", "famotidine", "ondansetron", "metoclopramide",

    # Others
    "acetaminophen", "gabapentin", "pregabalin",
    "levothyroxine", "prednisone", "methotrexate",
    "allopurinol", "colchicine", "lithium",
    "quetiapine", "risperidone", "olanzapine", "aripiprazole", "haloperidol",
    "sumatriptan", "rizatriptan",
    "sildenafil", "tadalafil", "tamsulosin",
    "phenytoin", "carbamazepine", "valproic acid", "lamotrigine",
    "levetiracetam", "topiramate",
    "tamoxifen", "anastrozole", "letrozole",
    "cyclosporine", "tacrolimus", "mycophenolate",
    "salbutamol", "salmeterol", "tiotropium", "fluticasone",
    "montelukast", "theophylline",
]


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

def run():
    print(f"OpenFDA ingestion pipeline")
    print(f"==========================")
    print(f"API key: {'YES (high rate limit)' if API_KEY else 'NO (1000/day limit)'}")
    print(f"Drugs to fetch: {len(DRUG_LIST)}")
    print()

    raw_labels = {}
    failed = []

    # Phase 1: Fetch all labels
    for i, drug in enumerate(DRUG_LIST, 1):
        print(f"[{i:3d}/{len(DRUG_LIST)}] Fetching: {drug}")
        label = fetch_label(drug)
        if label:
            text = extract_interaction_text(label)
            if text:
                raw_labels[drug] = text
                print(f"          ✓ {len(text)} chars of interaction text")
            else:
                print(f"          - no interaction section in label")
                failed.append(drug)
        else:
            print(f"          ✗ no label found")
            failed.append(drug)
        time.sleep(RATE_LIMIT_DELAY)

    print(f"\nPhase 1 complete: {len(raw_labels)}/{len(DRUG_LIST)} labels retrieved")
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

    # Phase 3: Build output structure
    print(f"\nPhase 3: Building output...")
    interactions = []
    for pair_set, severities in pairs.items():
        a, b = sorted(pair_set)
        # Take the most severe hint
        priority = ["contraindicated", "major", "moderate", "unknown"]
        sev = next((s for s in priority if s in severities), "unknown")
        interactions.append({
            "drugA": a,
            "drugB": b,
            "severity_hint": sev,
            "source": "openfda_label",
            # Keep a snippet from the source label for reference
            "snippet_a": _snippet(raw_labels.get(a, ""), b),
            "snippet_b": _snippet(raw_labels.get(b, ""), a),
        })

    output = {
        "generated": datetime.utcnow().isoformat() + "Z",
        "source": "OpenFDA drug label API",
        "drugs_fetched": len(raw_labels),
        "drugs_failed": len(failed),
        "interaction_count": len(interactions),
        "interactions": interactions,
        "disclaimer": (
            "Severity hints are heuristically inferred from label keywords "
            "and should be reviewed. This data supplements but does not "
            "replace curated interaction databases. Always verify high-stakes "
            "pairs against primary references."
        )
    }

    # Phase 4: Write JSON
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Done. Wrote {len(interactions)} interaction pairs to {OUTPUT_FILE}")
    print(f"  Severity breakdown:")
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
