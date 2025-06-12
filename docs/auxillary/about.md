# About TableVault

## Citations and Papers.

Feel free to read our paper to get more details about the project:

- ***TableVault: Managing Dynamic Data Collections for LLM-Augmented Workflows.*** Jinjin Zhao, and Sanjay Krishnan.

You can cite the paper with:

``` bibtex
 # t.b.d.

```

## Contact 

You can reach out through email: ***j2zhao@uchicago.edu***.

---

## (Potential) Future Extensions

- **More Backend Options**: We currently use pandas and CSV files as the backend of TableVault. While there are significant performance drawbacks to both frameworks, they were chosen due to their popularity. Alternative backends are definitively under consideration to enable more user freedom in how dataframes are stored, loaded, and queried.

- **SQL Support (Very Likely)**: TableReferences were designed to be easily understandable coming from Pandas/Python indexing. However, there's a good argument that SQL queries can also be used in the same function. In practice, this shouldn't (hopefully) be that difficult to implement, and will probably be added in the future.

- **Better Query Support**: Support for queries is still relatively limited. As we explore applications of TableVault, it will become more clear how data in TableVault should be accessed.

---