# Data Preparation & Experimentation: SEC EDGAR
### __:warning: This Repo is Work in Progress :warning:__
This repository will contain 2 things:
1. scripts to download SEC EDGAR data and format it for Neo4j loading, analytics, and GenAI. Specifically linking together form 10-K and form 13 data.
2. Exploratory notebooks and apps for testing loading and GenAI applications with the data.

## Background
Linking form10K documents and issuing companies from form13 is non-trivial since the two data sources use different identifiers and the SEC EDGAR API provides no way to directly resolve between them.  form10k filings are identified under CIK, an SEC system id, while form13 use the CUSIP identifier, another industry standard id, for issuers.

Thankfully, others have created scripts for mapping CIK and CUSIP via scraping other SEC filings. [This repository](https://github.com/leoliu0/cik-cusip-mapping) is one example, and they even provide a pre-calculated mapping in the form of a csv file. 

In this project we will build off that work by taking a CIK-CUSIP mapping as input then
1. Pulling and parsing text from form 10Ks
2. Pulling and staging holdings data from form 13s

These will be staged in a format conducive to loading into Neo4j and linking the data together.


## Prerequisites
You are required to have a CIK-CUSIP mapping csv file as input. If you do not have one and want to test out. see the `cik-sample-mapping.csv` file.  While you could use the [csv from the repo noted above](https://github.com/leoliu0/cik-cusip-mapping/blob/master/cik-cusip-maps.csv) it contains over 50k mappings which can take a while to process.

__[TODO] Add Python Package prereqs__

## Pulling and parsing text from form 10Ks
Currently, there are two command line utilities for this.  Will need to combine into one as we clean up.
1. `f10-get-urls.py` takes the cik-cusip mapping as input along with a date range and grabs the urls for raw 10k filings.  It then writes them to another csv.
2. `f10k-download-parse-format.py` takes the above output, downloads raw 10k files, parses out relevant 10K item text, and saves to json files. See __10K Notes__ below for more details on the reasoning behind parsing and item selection.

## Pulling and staging holdings data from form 13s
__[TODO]__

## 10K Notes

A [10K](https://www.investor.gov/introduction-investing/investing-basics/glossary/form-10-k) is a comprehensive report filed annually by a publicly traded company about its financial performance and is required by the U.S. Securities and Exchange Commission (SEC). The report contains a comprehensive overview of the company's business and financial condition and includes audited financial statements. While 10Ks contain images and table figures, they primarily consist of free-form text which is what we are interested in extracting here.

Raw 10K reports are structured in iXBRL, or Inline eXtensible Business Reporting Language, which is extremely verbose, containing more markup than actual text content, [here is an example from APPLE](https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/0000320193-22-000108.txt).

This makes raw 10K files very large, unwieldy, and inefficient for direct application of LLM or text embedding services. For this reason, the program contained here, `f10k-download-parse-format.py`, applies regex and NLP to parse out as much iXBRL and unnecessary content as possible to make 10K text useful.

In addition, `f10k-download-parse-format.py` also extracts only a subset of items from the 10K that we feel are most relevant for initial exploration and experimentation.  These are sections that discuss the overall business outlook and risk factors, specifically:

* __Item 1 – Business__
  This describes the business of the company: who and what the company does, what subsidiaries it owns, and what markets it operates in. It may also include recent events, competition, regulations, and labor issues. (Some industries are heavily regulated, have complex labor requirements, which have significant effects on the business.) Other topics in this section may include special operating costs, seasonal factors, or insurance matters.
* __Item 1A – Risk Factors__
  Here, the company lays out anything that could go wrong, likely external effects, possible future failures to meet obligations, and other risks disclosed to adequately warn investors and potential investors.
* __Item 7 – Management's Discussion and Analysis of Financial Condition and Results of Operations__
  Here, management discusses the operations of the company in detail by usually comparing the current period versus the prior period. These comparisons provide a reader an overview of the operational issues of what causes such increases or decreases in the business.
* __Item 7A – Quantitative and Qualitative Disclosures about Market Risks__.