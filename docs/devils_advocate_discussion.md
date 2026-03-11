# Devil's Advocate Discussion: dbt Architecture Decision

**Date:** March 11, 2026  
**Topic:** Whether to implement dbt transformation layer for Snowflake analytics  
**Participants:** Data team + Devil's Advocate agent

---

## Initial Proposal

Deploy dbt (data build tool) on Azure VM to create transformation layer:
- **Primary use case:** Create summary tables for web products to consume
- **Initial scope:** Two mart tables from chatbot data (monthly & total conversations by tenant)
- **Architecture:** Separate ANALYTICS database, all marts in ANALYTICS.PUBLIC schema
- **Governance:** GitHub PR approval workflow before changes merge

---

## Devil's Advocate Challenges

### Challenge 1: Is this the simplest solution?

**Question:** Two models consuming one source table → why create an entire new database and transformation stack? Could these just be views in AVA.PUBLIC?

**Response:** This is an MVP to test viability, with intent to scale up and eventually replace all R transformations.

### Challenge 2: What problem does this actually solve?

**Question:** chatbot_data R pipeline already: extracts → transforms → loads → reports. Moving transforms to dbt adds MORE moving parts, not fewer. What breaks in the current flow?

**Response (Key Insight):** 
> "The problem I'm solving is that **signal is stored deep in R code that only I can read.**"

Two primary problems:
1. **Bus factor = 1** — transformations locked in R tidyverse that few can read
2. **No governance transparency** — business logic changes happen invisibly in R scripts

### Challenge 3: Could you get governance without dbt?

**Question:** Extract SQL into standalone `.sql` files in chatbot_data repo, have PM review those via PR, run via existing R cron job. Achieves: ✅ SQL reviewability, ✅ PM approval workflow, ❌ no dbt maintenance overhead.

**Response:** 
> "Doesn't dbt have more of a pleasant UI? It feels like a more robust piece of tooling that is free, has fairly low infra overhead and allows for smart options in the future, no? Rather than homecooking my own?"

---

## Final Decision: Proceed with dbt

### Devil's Advocate Conclusion (Concession)

**Agreed. dbt is the right choice because:**

- ✅ **Industry standard** — any future analytics hire knows dbt
- ✅ **Free, maintained, documented** — not rolling your own transform framework  
- ✅ **Low infrastructure overhead** — Python/Snowflake/cron already exist, dbt just orchestrates SQL
- ✅ **Future optionality** — incremental models, snapshot tables, data quality tests, auto-documentation, lineage DAGs all come "for free" once foundation is laid
- ✅ **Pleasant UI** — dbt docs generate lineage graphs, CLI output is clean
- ✅ **Less tech debt than homecooking** — maintaining your own "SQL files in R" is MORE debt than adopting dbt

**Critical insight:** Homecooking governance around SQL files means maintaining your own:
- Orchestration logic
- Testing framework  
- Documentation patterns

dbt provides all of this as a standard, maintained toolkit.

---

## Approved Architecture

**ANALYTICS.PUBLIC** — Single schema for all dbt marts:
- **AVA.PUBLIC** → Raw chatbot data (owned by chatbot_data ETL)
- **CASEWORK.PUBLIC** → Raw casework data (owned by advicePro_queries ETL)  
- **ANALYTICS.PUBLIC** → ALL dbt marts (chatbot, casework, and unified)

**Benefits:**
- Clean separation: ETL repos own raw data databases, dbt owns analytics database
- No favoritism — ANALYTICS is its own domain, not "part of AVA"
- Simplest for consumers (Power BI, web products) — everything in one schema
- Standard dbt pattern: source data in raw databases, transformed data in analytics database
- Model organization happens via dbt folder structure (`marts/chatbot/`, `marts/casework/`, `marts/unified/`) not database schemas

---

## MVP Validation Criteria

This two-model MVP tests:

1. **Technical:** Can dbt connect to Snowflake and run successfully?
2. **Cultural:** Does the PM review workflow actually happen in practice?
3. **Product:** Do web products successfully consume the tables?

If any fail, we learn early with minimal investment. If all succeed, we have a foundation to migrate more transformations.

---

## Key Takeaway

**This is a governance play disguised as a tech migration.**

The real value isn't "dbt vs R" — it's:
- Reducing bus factor from 1 to many
- Making business logic changes transparent and reviewable
- Creating shared ownership of transformation logic between data and product teams

dbt is the right tool because it's purpose-built for this pattern and brings a full ecosystem of supporting features (testing, documentation, lineage) that would be painful to build yourself.

---

## Status

**Decision:** Approved — proceed with ANALYTICS.PUBLIC architecture  
**Next step:** Update dbt-asc repository files to reflect ANALYTICS database target  
**Deployment:** Pending Snowflake permissions setup and VM configuration
