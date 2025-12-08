
# How To... Videos - ECL Tips and Tricks

These videos offer tips and tricks for working with the ECL programming language.

---

## How to... Use Macros

- [Use ECL Macros Session 1 - A worked through case](https://www.youtube.com/watch?v=T_Of4xckuuY&list=PLONd-6DN_sz3QTzE5s_qbOSDJ8V-IEXUM&index=1)  
  Explanation of ECL (Enterprise Control Language) MACROS using a real-life case study. Compare attributes between two key builds where the number of attributes is so large that using MACROS is the only solution.
- [Use ECL Macros Session 2 - A worked through case](https://www.youtube.com/watch?v=jj9xmTaEzzw&list=PLONd-6DN_sz3QTzE5s_qbOSDJ8V-IEXUM&index=3)  
  Constructing a new output layer.
- [Use ECL Macros Session 3 - A worked through case](https://www.youtube.com/watch?v=zZsga8rabQI&list=PLONd-6DN_sz3QTzE5s_qbOSDJ8V-IEXUM&index=4)  
  Generating an email using ECL to act on the information discovered in the previous session.
- [Be clear about the context in which the ECL FUNCTIONMACROs run](https://www.youtube.com/watch?v=rZM7VhnpCP8&list=PLONd-6DN_sz2aHrCyFbvIU6Q33r55YzWk&index=5)  
  Demonstrates that ECL FUNCTIONMACROs compile in the context of the caller and not in the context of the ECL in which the FUNCTIONMACRO resides.
- [ECL #EXPORT and #EXPORTXML](https://www.youtube.com/watch?v=k8wCx-5FynQ&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=17)  
  Processing record structures at compile time using MACROS.
- [Understand the idiosyncrasies of ECL MACROs and #EXPAND](https://www.youtube.com/watch?v=XRjSDHkA-5A&list=PLONd-6DN_sz2aHrCyFbvIU6Q33r55YzWk&index=11)  
  Where #EXPAND can and can't be used in a macro.

---

## How to... Use ECL Built-in Functions

- [PROJECT](https://www.youtube.com/watch?v=iRpbeqqedbw&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=1)  
  Converting records in a dataset from one format to another.
- [HOW ECL PROJECT is different from other built-ins](https://www.youtube.com/watch?v=o4j2TPGE8N8&list=PLONd-6DN_sz2aHrCyFbvIU6Q33r55YzWk&index=4)  
  The ECL PROJECT built-in, unlike others, accepts a single Record as input.
- [TABLE (Vertical Slice)](https://www.youtube.com/watch?v=qV7PGXcfAuw&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=2)  
  Taking an input dataset in one format and generating an output dataset in another format.
- [TABLE (CrossTab)](https://www.youtube.com/watch?v=UU7P-9ilZbA&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=3)  
  Getting statistics on input datasets grouped by fields within the dataset.
- [Use Aggregates on subsets of data in a cross-tab report](https://www.youtube.com/watch?v=9Yy4UVZMC7w&list=PLONd-6DN_sz2aHrCyFbvIU6Q33r55YzWk&index=2)  
  Difference between COUNT operator and other operators in ECL cross-tab TABLE function.
- [ITERATE](https://www.youtube.com/watch?v=G2ulKC_rXFU&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=4)  
  Processes all records in the recordset one pair at a time.
- [PROCESS (Part 1)](https://www.youtube.com/watch?v=ZeczBOz8ulQ&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=5)  
  Processes all records in the recordset one pair at a time using datasettransform.
- [PROCESS (Part 2)](https://www.youtube.com/watch?v=vRA-7D6_mVQ&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=6)  
  Continuation of Part 1.
- [GRAPH](https://www.youtube.com/watch?v=O8L83FxAa6s&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=7)  
  Search a dataset with different attributes/dimensions.
- [NORMALIZE (COUNT Variant)](https://www.youtube.com/watch?v=M9RGTHaNGqo&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=8)  
  Simplest NORMALIZE function variant.
- [NORMALIZE (Child Dataset Extraction)](https://www.youtube.com/watch?v=gGZDXI1oboQ&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=9)  
  Extract child datasets from hierarchical dataset.
- [NORMALIZE (Additional Information)](https://www.youtube.com/watch?v=hWMS2cq6GDU&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=10)  
  Additional items such as using child datasets inside child datasets.
- [DENORMALIZE (Single Record Presentation Variant)](https://www.youtube.com/watch?v=FsbY45jx00U&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=11)  
  Inverse to NORMALIZE.
- [DENORMALIZE (GROUP Presentation Variant)](https://www.youtube.com/watch?v=gfA-tQJehGQ&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=12)  
  Inverse to NORMALIZE for child datasets.
- [ECL DICTIONARY (Examples of use)](https://www.youtube.com/watch?v=Uudr-TaCn3A&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=13)  
  Fast lookup for very large tables/datasets.
- [Working with distributed data (TABLE, ROLLUP and AGGREGATE)](https://www.youtube.com/watch?v=6nmCXH8GJaY&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=14)  
  How to mine data using HPCC Systems Architecture.
- [ROLLUP (Part 1)](https://www.youtube.com/watch?v=LE5HDegz5II&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=15)  
  Deduplicate records and capture information.
- [ROLLUP (Part 2)](https://www.youtube.com/watch?v=E-R138uuR3M&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=16)  
  Preserve information between iterations.
- [DEDUP](https://www.youtube.com/watch?v=T7rUaEaKfKg&list=PLONd-6DN_sz0i5zW0YFLhHTw7PETU5Rn5&index=18)  
  Evaluates recordset for duplicate records.
- [Be wary of using the Compiler hint DISTRIBUTED in ECL](https://www.youtube.com/watch?v=8M3kE8MJj_k&list=PLONd-6DN_sz2aHrCyFbvIU6Q33r55YzWk&index=6)  
  DISTRIBUTED is not the same as DISTRIBUTE command.
- [Find a Better way to optimise out file re-distributions using DISTRIBUTED](https://www.youtube.com/watch?v=zUhwVQM5gts&list=PLONd-6DN_sz2aHrCyFbvIU6Q33r55YzWk&index=7)  
  Best way to use DISTRIBUTED safely.

---

## Learn more tips and tricks

- [Understand the differences Between Build and definition files in HPCC Systems](https://www.youtube.com/watch?v=HxO63O0kAEA&list=PLONd-6DN_sz2aHrCyFbvIU6Q33r55YzWk&index=1)  
  To anyone new to HPCC Systems and the ECL language, the way HPCC Systems behaves differently with the two concepts of Actions and Definitions can be very confusing. Learn about the behavior used with both and find out more about why HPCC Systems runs this way.
- [Explicit return types from functions](https://www.youtube.com/watch?v=TiIxwb7Tl1U&list=PLONd-6DN_sz2aHrCyFbvIU6Q33r55YzWk&index=3)  
  Find out how defaulting return types form functions degrades the compilers ability to correctly report errors.
- [Understand the Distribution of data on HPCC Systems, with an example](https://www.youtube.com/watch?v=K1GVb3AK91E)  
  Describes the Distribution of Logical files onto multiple nodes of an HPCC Systems environment. Also find out more about the measure of SKEW from an even distribution as defined for HPCC Systems.
- [Use ECL/HPCC Systems Events, Including Scheduling Events](https://www.youtube.com/watch?v=ca-XrunU2yU&list=PLONd-6DN_sz2aHrCyFbvIU6Q33r55YzWk&index=10)  
  Demonstration of creating, fielding and firing events in HPCC Systems. Also includes some additional notes on scheduling events (CRON), including reading the output of one workunit in another.
- [Use ECL Watch Advanced Search](https://www.youtube.com/watch?v=U73ygvfi2PE)  
  Examines some of the advanced functionality built into the ECL Watch Global Search box. Demonstrates how to use prefixes to filter for file types, and navigate to their corresponding pages and show how to filter for specific dates or date ranges.
