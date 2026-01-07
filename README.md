# MLVault

Version 2.0 of TableVault. Going through a standard ML rebrand! 

# Changes from v1.0

Planned Changes:

- Data is stored by data type rather than by instance.

- Tables are stored relationally rather than as DataFrames.

- First level support for arrays, and text files (hence name change from tables).

- Logs are embedded with the data for direct provenance access.

- API changes to match industry orchestration standards.

# New Core Feature: Data Queries

The reason for the redesign is that we want to design the metadata layer to support robust queries from the ground up.

The API is planned to support:

- Vector search over data descriptions

- Reproducible search (with timestamps)

- Direct and indirect data lineage (based on code traces)

I would also like to include:

- Experimental LLM Layer to support natural language queries

def _ask_model():
    system_prompt = ""
    You are an critic agent for legal defence.

    You will be give:
    - Evidence list
    - Defense statement
    - Problem

    You want to return an outpput with the exact template:

    {
        Score: int,
        Critic: str

    }

    Det


    """