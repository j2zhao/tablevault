# About TableVault

## Citations and Papers

Feel free to read our paper to get more details about the project:

=== "Paper"

    > **TableVault: Managing Dynamic Data Collections for LLM-Augmented Workflows**  
    > *Jinjin Zhao & Sanjay Krishnan*  
    > *NOVAS Workshop @ SIGMOD 2025*

=== "Bibtex"

    ```bibtex
    @misc{zhao2025tablevaultmanagingdynamicdata,
        title={TableVault: Managing Dynamic Data Collections for LLM-Augmented Workflows}, 
        author={Jinjin Zhao and Sanjay Krishnan},
        year={2025},
        eprint={2506.18257},
        archivePrefix={arXiv},
        primaryClass={cs.DB},
        url={https://arxiv.org/abs/2506.18257}, 
    }
    ```

## Contact

You can reach out through email: ***j2zhao@uchicago.edu***.

---

## (Potential) Future Extensions

-   **More Backend Options**: We currently use pandas and CSV files as the backend for TableVault. While there are significant performance drawbacks to both frameworks, they were chosen for their popularity. Alternative backends are definitely under consideration to enable more user freedom in how dataframes are stored, loaded, and queried.

-   **SQL Support (Very Likely)**: TableReferences were designed to be easily understandable for users familiar with Pandas/Python indexing. However, there's a good argument that SQL queries can also be used for the same purpose. In practice, this shouldn't (hopefully) be too difficult to implement and will probably be added in the future.

-   **Better Query Support**: Support for queries is still relatively limited. As we explore applications of TableVault, it will become clearer how data in TableVault should be accessed.
