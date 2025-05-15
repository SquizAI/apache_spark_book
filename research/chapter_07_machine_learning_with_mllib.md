# Chapter 7: Machine Learning with MLlib

## Topic

This chapter provides a comprehensive guide to Apache Spark's machine learning library, MLlib. It will differentiate between the older RDD-based `spark.mllib` and the newer DataFrame-based `spark.ml` (which is the focus). Core concepts of `spark.ml` like Transformers, Estimators, Pipelines, Parameters, and Evaluators will be explained. We'll cover MLlib's data types, delve into feature engineering techniques (extraction, transformation, selection), explore common algorithms for classification, regression, clustering, and collaborative filtering, and discuss how to build, evaluate, and tune ML models using Pipelines and tools like `CrossValidator`. Model persistence will also be addressed.

## Keywords

Spark MLlib, `spark.ml`, `spark.mllib`, Machine Learning, DataFrame, Pipeline, Estimator, Transformer, Parameter, Evaluator, Feature Engineering, Feature Extraction, Feature Transformation, Feature Selection, `Vector`, `DenseVector`, `SparseVector`, `LabeledPoint`, TF-IDF, Word2Vec, CountVectorizer, StandardScaler, MinMaxScaler, Normalizer, PCA, StringIndexer, IndexToString, OneHotEncoder, VectorAssembler, Bucketizer, ElementwiseProduct, SQLTransformer, Feature Selection, Classification, Regression, Clustering, Collaborative Filtering, Logistic Regression, Decision Trees, Random Forests, Gradient-Boosted Trees (GBTs), Naive Bayes, Multilayer Perceptron, Linear Regression, Generalized Linear Regression (GLR), K-Means, Latent Dirichlet Allocation (LDA), Alternating Least Squares (ALS), Model Training, Model Evaluation, Hyperparameter Tuning, `CrossValidator`, `TrainValidationSplit`, Model Persistence, Saving Models, Loading Models.

## Structure

1.  [Introduction to Spark MLlib](#introduction-to-spark-mllib)
    *   What is MLlib?
    *   Goals and Design Philosophy
    *   The Two MLlib Packages: `spark.mllib` (RDD-based) vs. `spark.ml` (DataFrame-based)
    *   Focus on `spark.ml` for New Development
2.  [Core Concepts in `spark.ml`: The Pipeline API](#core-concepts-in-sparkml-the-pipeline-api)
    *   DataFrame: The primary data structure for `spark.ml`.
    *   Transformer: An algorithm that can transform one DataFrame into another DataFrame.
    *   Estimator: An algorithm that can be `fit` on a DataFrame to produce a Transformer (e.g., a learning algorithm).
    *   Pipeline: Chains multiple Transformers and Estimators together to specify an ML workflow.
    *   Parameter (`Param`): A named parameter for an Estimator or Transformer with self-contained documentation.
    *   Evaluator: Used to evaluate model performance based on specific metrics.
3.  [Data Types for Machine Learning in Spark](#data-types-for-machine-learning-in-spark)
    *   Vectors: `DenseVector` and `SparseVector`.
    *   `LabeledPoint` (Primarily for `spark.mllib`).
    *   DataFrame Columns for Features and Labels in `spark.ml` (typically a "features" `Vector` column and a "label" `Double` column).
4.  [Feature Engineering](#feature-engineering)
    *   Importance of Feature Engineering.
    *   Feature Extraction:
        *   TF-IDF (Term Frequency-Inverse Document Frequency): `HashingTF`, `IDF`.
        *   Word2Vec: `Word2Vec`.
        *   CountVectorizer: `CountVectorizer`.
    *   Feature Transformation:
        *   Scaling: `StandardScaler`, `MinMaxScaler`, `MaxAbsScaler`, `Normalizer`.
        *   Dimensionality Reduction: `PCA` (Principal Component Analysis).
        *   Categorical Feature Handling: `StringIndexer`, `IndexToString`, `OneHotEncoder`.
        *   Assembling Features: `VectorAssembler` (combining multiple columns into a single feature vector).
        *   Discretization: `Bucketizer`.
        *   Interaction: `ElementwiseProduct`.
        *   Custom Transformations: `SQLTransformer`.
    *   Feature Selection:
        *   `VectorSlicer`.
        *   ChiSqSelector, RFormula (brief mention).
    *   Code Examples for common feature engineering tasks (Scala, Python).
5.  [Machine Learning Algorithms in `spark.ml`](#machine-learning-algorithms-in-sparkml)
    *   Overview of Algorithm Categories.
    *   Classification Algorithms:
        *   Logistic Regression (`LogisticRegression`).
        *   Decision Trees (`DecisionTreeClassifier`).
        *   Random Forests (`RandomForestClassifier`).
        *   Gradient-Boosted Trees (GBTs) (`GBTClassifier`).
        *   Naive Bayes (`NaiveBayes`).
        *   Multilayer Perceptron Classifier (`MultilayerPerceptronClassifier`).
    *   Regression Algorithms:
        *   Linear Regression (`LinearRegression`).
        *   Generalized Linear Regression (`GeneralizedLinearRegression`).
        *   Decision Trees (`DecisionTreeRegressor`).
        *   Random Forests (`RandomForestRegressor`).
        *   Gradient-Boosted Trees (GBTs) (`GBTRegressor`).
        *   Isotonic Regression (`IsotonicRegression`).
    *   Clustering Algorithms:
        *   K-Means (`KMeans`).
        *   Latent Dirichlet Allocation (LDA) (`LDA`).
        *   Bisecting K-Means (`BisectingKMeans`).
        *   Gaussian Mixture Model (GMM) (`GaussianMixture`).
    *   Collaborative Filtering:
        *   Alternating Least Squares (ALS) (`ALS`).
    *   Code snippets for instantiating and setting parameters for common algorithms.
6.  [Building Machine Learning Pipelines](#building-machine-learning-pipelines)
    *   Defining Pipeline Stages (sequence of Transformers and Estimators).
    *   Creating a `Pipeline` object.
    *   Training the Pipeline: `pipeline.fit(trainingData)` returns a `PipelineModel`.
    *   Making Predictions: `pipelineModel.transform(testData)`.
    *   Benefits of using Pipelines for workflow management and reproducibility.
    *   Code Example of a complete pipeline (Scala, Python).
7.  [Model Evaluation](#model-evaluation)
    *   The Role of Evaluators.
    *   Common Evaluators:
        *   `RegressionEvaluator` (Metrics: RMSE, MSE, R2, MAE).
        *   `BinaryClassificationEvaluator` (Metrics: Area Under ROC, Area Under PR).
        *   `MulticlassClassificationEvaluator` (Metrics: F1, Weighted Precision, Weighted Recall, Accuracy).
        *   `ClusteringEvaluator` (Metrics: Silhouette score).
    *   Making predictions and then using evaluators.
    *   Code Examples for evaluating different model types (Scala, Python).
8.  [Hyperparameter Tuning](#hyperparameter-tuning)
    *   Importance of Tuning Hyperparameters.
    *   Tools for Automated Tuning:
        *   `CrossValidator`: Performs k-fold cross-validation.
            *   Requires an Estimator, a set of Parameter Maps (`ParamMap`), and an Evaluator.
        *   `TrainValidationSplit`: Simpler, splits data into one training and one validation set.
            *   Requires an Estimator, a set of Parameter Maps, an Evaluator, and a `trainRatio`.
    *   Setting up `ParamGridBuilder` for defining the search space.
    *   Fitting `CrossValidator` or `TrainValidationSplit` to find the best model.
    *   Code Examples for hyperparameter tuning (Scala, Python).
9.  [Model Persistence: Saving and Loading Models and Pipelines](#model-persistence-saving-and-loading-models-and-pipelines)
    *   Need for Saving and Loading trained models and pipelines.
    *   `save(path)` and `load(path)` methods for Transformers, Estimators, Models, and Pipelines.
    *   Considerations for portability and compatibility (e.g., Spark versions, language differences).
    *   Brief mention of MLeap for wider deployment (optional).
    *   Code Examples for saving and loading (Scala, Python).
10. [Chapter Summary](#chapter-summary)

---

## Introduction to Spark MLlib

### What is MLlib?
MLlib is Apache Spark's scalable machine learning library. It provides common learning algorithms and utilities, including classification, regression, clustering, collaborative filtering, dimensionality reduction, as well as lower-level optimization primitives and higher-level pipeline APIs.

### Goals and Design Philosophy
MLlib aims to make practical machine learning scalable and easy. Key goals include:
*   **Scalability:** Designed to run on large datasets in distributed environments.
*   **Ease of Use:** Provides high-level APIs that simplify the development of ML applications.
*   **Performance:** Leverages Spark's distributed in-memory computation for speed.
*   **Integration:** Seamlessly integrates with other Spark components like Spark SQL and Spark Streaming.

### The Two MLlib Packages: `spark.mllib` (RDD-based) vs. `spark.ml` (DataFrame-based)
MLlib comes with two main packages:
*   **`spark.mllib`**: This is the original API built on top of RDDs. It is now in maintenance mode. While it contains a wide range of algorithms, new feature development is primarily focused on `spark.ml`.
*   **`spark.ml`**: This is the newer API built on top of DataFrames. It provides a more uniform and higher-level API centered around the concept of Pipelines, making it easier to construct, evaluate, and tune complex ML workflows. It offers better performance due to DataFrame optimizations (Catalyst and Tungsten) and is the recommended API for new applications.

This chapter will primarily focus on the `spark.ml` package.

## Core Concepts in `spark.ml`: The Pipeline API

The `spark.ml` API is designed around the concept of Pipelines, which are sequences of stages. These stages can be either Transformers or Estimators.

*   **DataFrame**: `spark.ml` uses DataFrames from Spark SQL as its primary data representation. DataFrames hold various data types, including vectors for features. Typically, ML algorithms expect input DataFrames to have a "features" column and a "label" column (for supervised learning).
*   **Transformer**: A Transformer is an algorithm that can transform one DataFrame into another. For example, a feature transformer might take a DataFrame, read a column, and append a new column with the transformed feature. A model is also a Transformer that takes a DataFrame with features and produces a DataFrame with predictions.
*   **Estimator**: An Estimator is an algorithm that can be fit on a DataFrame to produce a Transformer. For example, a learning algorithm like `LogisticRegression` is an Estimator. Calling its `fit()` method on a DataFrame (training data) produces a `LogisticRegressionModel`, which is a Transformer.
*   **Pipeline**: A Pipeline chains multiple Transformers and Estimators together to create an ML workflow. For example, a pipeline might consist of stages for feature extraction, feature transformation, and model training.
*   **Parameter (`Param`)**: All Transformers and Estimators share a common API for specifying parameters. A `Param` is a named parameter with self-contained documentation.
*   **Evaluator**: An Evaluator is used to assess the performance of a trained model. It takes a DataFrame with predictions and actual labels, and outputs a metric (e.g., accuracy, RMSE).

## Data Types for Machine Learning in Spark

*   **Vectors**: MLlib uses `org.apache.spark.ml.linalg.Vector` (in `spark.ml`) or `org.apache.spark.mllib.linalg.Vector` (in `spark.mllib`) for feature vectors.
    *   `DenseVector`: Stores all its elements in an array of doubles.
    *   `SparseVector`: Stores only the non-zero values and their indices to save space, which is efficient for high-dimensional, sparse data.
*   **`LabeledPoint` (`spark.mllib`)**: A data type representing a feature vector and its associated label. Used in the RDD-based API.
    ```scala
    // Scala - mllib
    // import org.apache.spark.mllib.linalg.Vectors
    // import org.apache.spark.mllib.regression.LabeledPoint
    // val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    // val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    ```
*   **DataFrame Columns (`spark.ml`)**: In `spark.ml`, features are typically stored in a `Vector` column (often named "features") and labels in a `Double` column (often named "label") within a DataFrame.

## Feature Engineering

Feature engineering is the process of using domain knowledge to create features that make machine learning algorithms work. `spark.ml` provides a rich set of tools for this.

### Feature Extraction
*   **TF-IDF (`HashingTF`, `IDF`)**: Used for text data. `HashingTF` maps sequences of terms to fixed-length feature vectors. `IDF` then rescales feature vectors based on how often terms appear in the entire corpus.
*   **Word2Vec (`Word2Vec`)**: Computes distributed vector representations of words.
*   **CountVectorizer (`CountVectorizer`)**: Converts text documents to vectors of token counts.

### Feature Transformation
*   **Scaling (`StandardScaler`, `MinMaxScaler`, `MaxAbsScaler`, `Normalizer`)**: Scales features to a certain range or distribution. `StandardScaler` standardizes features by removing the mean and scaling to unit variance. `MinMaxScaler` rescales features to a common range [min, max]. `Normalizer` scales individual samples to have unit L^p norm.
*   **Dimensionality Reduction (`PCA`)**: Reduces the number of features while preserving variance.
*   **Categorical Feature Handling**: 
    *   `StringIndexer`: Encodes a string column of labels into a column of label indices.
    *   `IndexToString`: Reverses `StringIndexer`, mapping label indices back to original string labels.
    *   `OneHotEncoder`: Maps a column of category indices to a column of binary vectors, with at most a single one-value.
*   **Assembling Features (`VectorAssembler`)**: A crucial transformer that combines a list of columns into a single vector column (usually named "features").
*   **Discretization (`Bucketizer`)**: Transforms a column of continuous features into a column of feature buckets.
*   **Interaction (`ElementwiseProduct`)**: Computes the element-wise product between two feature vectors.
*   **Custom Transformations (`SQLTransformer`)**: Allows applying SQL expressions to transform DataFrames.

### Feature Selection
*   **`VectorSlicer`**: Takes a feature vector and outputs a new feature vector with a sub-array of the original features.
*   **`ChiSqSelector`**: Performs Chi-Squared feature selection.

## Machine Learning Algorithms in `spark.ml`

`spark.ml` supports a wide array of algorithms.

### Classification Algorithms
*   `LogisticRegression`: For binary and multinomial classification.
*   `DecisionTreeClassifier`: Based on decision tree learning.
*   `RandomForestClassifier`: Ensemble of decision trees.
*   `GBTClassifier`: Gradient-Boosted Trees.
*   `NaiveBayes`: Probabilistic classifier based on Bayes' theorem.
*   `MultilayerPerceptronClassifier`: Feedforward artificial neural network.

### Regression Algorithms
*   `LinearRegression`: Standard linear regression.
*   `GeneralizedLinearRegression` (GLR): Flexible generalization of ordinary linear regression.
*   `DecisionTreeRegressor`, `RandomForestRegressor`, `GBTRegressor`: Tree-based regression models.
*   `IsotonicRegression`: Fits a non-decreasing function to data.

### Clustering Algorithms
*   `KMeans`: Partitions data points into K clusters.
*   `LDA` (Latent Dirichlet Allocation): Topic modeling for text data.
*   `BisectingKMeans`: A hierarchical clustering algorithm.
*   `GaussianMixture` (GMM): Models data as a mixture of Gaussian distributions.

### Collaborative Filtering
*   `ALS` (Alternating Least Squares): Commonly used for recommender systems.

## Building Machine Learning Pipelines

A `Pipeline` in `spark.ml` consists of a sequence of stages, where each stage is either a `Transformer` or an `Estimator`.

```scala
// Scala Example: Basic Pipeline
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, IDF}
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("PipelineExample").master("local").getOrCreate()

// Prepare training documents from a list of (id, text, label) tuples.
val training = spark.createDataFrame(Seq(
  (0L, "a b c d e spark", 1.0),
  (1L, "b d", 0.0),
  (2L, "spark f g h", 1.0),
  (3L, "hadoop mapreduce", 0.0)
)).toDF("id", "text", "label")

// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("rawFeatures")
val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)
val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, lr))

// Fit the pipeline to training documents.
val model = pipeline.fit(training)

// Prepare test documents, which are unlabeled (id, text) tuples.
val test = spark.createDataFrame(Seq(
  (4L, "spark i j k"),
  (5L, "l m n"),
  (6L, "spark hadoop spark"),
  (7L, "apache hadoop")
)).toDF("id", "text")

// Make predictions on test documents.
val predictions = model.transform(test)
predictions.select("id", "text", "probability", "prediction").show()
spark.stop()
```

## Model Evaluation

Evaluators are used to assess the quality of the model's predictions.
*   `RegressionEvaluator`: Metrics like Root Mean Squared Error (RMSE), Mean Squared Error (MSE), R-squared (R2), Mean Absolute Error (MAE).
*   `BinaryClassificationEvaluator`: Area Under ROC (AUC-ROC), Area Under Precision-Recall Curve (AUC-PR).
*   `MulticlassClassificationEvaluator`: Metrics like F1-score, precision, recall, accuracy.
*   `ClusteringEvaluator`: Silhouette score.

```scala
// Scala Example: Binary Classification Evaluation
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
// 'predictions' DataFrame obtained from model.transform()
// It should contain a 'label' column and a 'rawPrediction' or 'probability' column
val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("rawPrediction") // or "probabilityCol"
  .setMetricName("areaUnderROC")

// val accuracy = evaluator.evaluate(predictions)
// println(s"Test Error = ${1.0 - accuracy}") 
// If using areaUnderROC, higher is better. For error, 1.0 - metric might be used if metric is accuracy.
// println(s"Area Under ROC = ${evaluator.evaluate(predictions)}") 
```

## Hyperparameter Tuning

Finding the best set of hyperparameters for an ML algorithm is crucial for model performance.
*   **`ParamGridBuilder`**: Used to construct a grid of parameters to search over.
*   **`CrossValidator`**: Performs k-fold cross-validation. It splits the data into k folds, trains on k-1 folds, and evaluates on the remaining fold. It repeats this k times and averages the evaluation metrics to select the best `ParamMap`.
*   **`TrainValidationSplit`**: Simpler and less expensive than `CrossValidator`. It splits the data into a single training set and a single validation set. It trains on the training set and evaluates on the validation set to select the best `ParamMap`.

```scala
// Scala Example: CrossValidator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

// ... (pipeline and evaluator setup as before)
// val lr = new LogisticRegression().setMaxIter(10)
// val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, lr))

val paramGrid = new ParamGridBuilder()
  .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
  .addGrid(lr.regParam, Array(0.1, 0.01))
  .build()

// val evaluator = new BinaryClassificationEvaluator() // Set up as before

val cv = new CrossValidator()
  .setEstimator(pipeline) // Estimator can be an algorithm or an entire Pipeline
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)  // Use 3+ folds in practice
  .setParallelism(2) // Evaluate up to 2 parameter settings in parallel

// Run cross-validation, and choose the best set of parameters.
// val cvModel = cv.fit(training)

// Make predictions on test documents. cvModel uses the best model found (PipelineModel).
// val cvPredictions = cvModel.transform(test)
// cvPredictions.select("id", "text", "probability", "prediction").show()
```

## Model Persistence: Saving and Loading Models and Pipelines

Spark `spark.ml` models and pipelines can be saved to disk for later use.
*   **Saving**: `model.save("path/to/myModel")` or `pipeline.save("path/to/myPipeline")`.
*   **Loading**: `val sameModel = PipelineModel.load("path/to/myModel")` or `val samePipeline = Pipeline.load("path/to/myPipeline")`.

This is crucial for deploying models into production or reusing them without retraining.

## Chapter Summary

This chapter provided an extensive overview of Spark MLlib, focusing on the modern `spark.ml` DataFrame-based API. We explored the fundamental concepts of Transformers, Estimators, and Pipelines, which form the backbone of building machine learning workflows in Spark. We covered essential data types and a wide array of feature engineering techniques for preparing data. A survey of common algorithms for classification, regression, clustering, and collaborative filtering was presented. Key practical aspects such as model evaluation using various metrics, hyperparameter tuning with `CrossValidator` and `TrainValidationSplit`, and model persistence for saving and loading trained models and pipelines were also detailed. With these tools and concepts, data scientists and engineers can leverage Spark to build scalable and robust machine learning applications.

---

## Further Reading and Resources

For a deeper dive into the topics covered in this chapter and to explore related concepts, please refer to the following resources:

*   **Consolidated References:** For a comprehensive list of official documentation, books, and community resources, see the [References](../references.md) section of this book.
*   **Glossary of Terms:** To understand key terminology used in Apache Spark, consult the [Glossary](../glossary.md).
*   **Book Index:** For a detailed index of topics, refer to the [Index](../index.md).

*(Specific links from `references.md` or pointers to other chapters can be added here if highly relevant to this specific chapter's content.)*
