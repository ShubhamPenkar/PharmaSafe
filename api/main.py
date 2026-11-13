"""
PharmaSafe API: read-only service over the Gold layer (nothing is recomputed per request).

    uvicorn api.main:app --reload            # docs at /docs
Environment: PHARMASAFE_GOLD_DIR, PHARMASAFE_SILVER_DIR, PHARMASAFE_SNAPSHOT (default: data/gold, data/silver, latest)
"""

import os
import sys
from pathlib import Path
from typing import Annotated, Literal, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.serving.gold_store import GoldStore, SORTABLE
from src.signal_detection.explain import LIMITATIONS

RolePolicy = Literal["primary", "all_suspect", "all_reported"]
Status = Literal["POTENTIAL_SIGNAL", "NOT_FLAGGED"]

app = FastAPI(
    title="PharmaSafe API",
    version="1.0.0",
    description=("Disproportionality analysis of FDA FAERS/AEMS spontaneous reports. Results are potential safety signals "
                 "for further investigation, not evidence of causation and not patient risk estimates."),
)


def build_store() -> GoldStore:
    root = Path(os.environ.get("PHARMASAFE_GOLD_DIR", PROJECT_ROOT / "data/gold"))
    silver = Path(os.environ.get("PHARMASAFE_SILVER_DIR", PROJECT_ROOT / "data/silver"))
    return GoldStore(root, os.environ.get("PHARMASAFE_SNAPSHOT", "latest"), reports_dir=PROJECT_ROOT / "reports", silver_dir=silver)


def get_store(request: Request) -> GoldStore:
    if getattr(request.app.state, "store", None) is None:
        try:
            request.app.state.store = build_store()
        except FileNotFoundError as ex:
            raise HTTPException(status_code=503, detail=str(ex))
    return request.app.state.store


Store = Annotated[GoldStore, Depends(get_store)]


@app.get("/health")
def health(request: Request):
    try:
        get_store(request)
        return {"status": "ok", "gold_available": True}
    except HTTPException:
        return {"status": "degraded", "gold_available": False}


@app.get("/overview")
def overview(store: Store, role_policy: RolePolicy = "primary"):
    return {
        **store.overview(role_policy),
        "top_reactions": store.top_reactions(15), "top_drugs": store.top_drugs(15, role_policy),
        "top_pairs": store.top_pairs(15, role_policy), "outcomes": store.outcomes(),
        "signal_status_counts": store.signal_status_counts(role_policy), "quarterly_trends": store.quarterly_trends(),
        "limitations": LIMITATIONS,
    }


@app.get("/drugs")
def search_drugs(store: Store, q: Annotated[str, Query(min_length=2, description="ingredient or reported product name")], limit: Annotated[int, Query(ge=1, le=100)] = 25):
    return {"query": q, "items": store.search_drugs(q, limit)}


@app.get("/drugs/{drug_id}")
def drug(store: Store, drug_id: str, role_policy: RolePolicy = "primary", top_reactions: Annotated[int, Query(ge=1, le=200)] = 25):
    detail = store.drug_detail(drug_id, role_policy)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Drug '{drug_id}' not found; use /drugs?q= to search")
    detail["reactions"] = store.signals(role_policy, drug=drug_id, status=None, exact=True, sort_by="observed_cases", limit=top_reactions)["items"]
    return detail


@app.get("/reactions")
def reactions(store: Store, q: Optional[str] = None, limit: Annotated[int, Query(ge=1, le=500)] = 50):
    items = store.top_reactions(5000)
    if q:
        items = [r for r in items if q.upper() in r["pt"]]
    return {"items": items[:limit]}


@app.get("/signals")
def signals(store: Store, role_policy: RolePolicy = "primary", drug: Optional[str] = None, reaction: Optional[str] = None,
            status: Optional[Status] = "POTENTIAL_SIGNAL", min_cases: Annotated[int, Query(ge=0)] = 0,
            sort_by: str = "ROR_CI_lower", descending: bool = True,
            limit: Annotated[int, Query(ge=1, le=1000)] = 100, offset: Annotated[int, Query(ge=0)] = 0):
    if sort_by not in SORTABLE:
        raise HTTPException(status_code=422, detail=f"sort_by must be one of {sorted(SORTABLE)}")
    return store.signals(role_policy, drug, reaction, status, min_cases, sort_by, descending, limit, offset)


@app.get("/signals/{drug_id}/{reaction_term}")
def signal_detail(store: Store, drug_id: str, reaction_term: str, role_policy: RolePolicy = "primary"):
    found = store.signal(drug_id, reaction_term, role_policy)
    if found is None:
        raise HTTPException(status_code=404, detail="No stored drug-event pair (fewer cases than the storage minimum, or unknown terms)")
    return found


@app.get("/trends")
def trends(store: Store, drug: Optional[str] = None, reaction: Optional[str] = None):
    out = {"quarterly_trends": store.quarterly_trends()}
    if drug:
        detail = store.drug_detail(drug)
        out["drug_quarterly_cases"] = detail["quarterly_cases"] if detail else []
        out["quarterly_signals"] = store.temporal_signals(drug, reaction)
    if reaction:
        out["reaction_quarterly_cases"] = store.quarterly_reaction_counts(reaction)
    out["signal_persistence"] = store.persistence(drug, limit=100)
    return out


@app.get("/combinations")
def combinations(store: Store, drug: Optional[str] = None, status: Optional[Status] = None, limit: Annotated[int, Query(ge=1, le=1000)] = 100):
    return {
        "note": "Co-reporting of drugs in a case is not evidence of a pharmacological interaction.",
        "top_pairs": store.top_combinations(25), "signals": store.combinations(drug, status, limit),
    }


@app.get("/data-quality")
def data_quality(store: Store):
    return store.data_quality() or {"detail": "No data quality metrics found; run the full pipeline."}
