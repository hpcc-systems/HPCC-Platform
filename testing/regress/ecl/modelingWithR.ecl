/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//class=embedded
//class=embedded-r
//class=3rdparty

// The following script uses embeded R code to train some models on data passed to R
// from HPCC. The results are then returned to HPCC and output.

IMPORT R;

////// Get some example data from R (in practice this may be a dataset on the ECL side)

// Construct a record structure to house the data
irisRec := RECORD
  REAL8     sepallength;
  REAL8     sepalwidth;
  REAL8     petallength;
  REAL8     petalwidth;
  UNSIGNED1 label;
END;

// Get built-in iris dataset from R
DATASET(irisRec) getIrisData() := EMBED(R)
  # iris is a built-in dataset, included in the base R distribution
  irisData <- iris
  irisData$label <- as.integer(irisData$Species == "versicolor")
  irisData$Species <- NULL
  irisData
ENDEMBED;

irisData := getIrisData();
OUTPUT(irisData);

////// Run some analysis in R

// Construct a record structure to house the data with predictions
irisPredictedRec := RECORD
  UNSIGNED1 label;
  REAL8     lmPreds;
  REAL8     glmPreds;
END;

// Model summary data will be stored and formatted using a single-column dataset
irisFitRec := RECORD
  STRING100 fit;
END;

// We return a record containing predictions and fit
resultRec := RECORD
  DATASET(irisPredictedRec) predictions;
  DATASET(irisFitRec) fit;
END;

// Main embedded R script for building and scoring models
resultRec runAnalyses(DATASET(irisRec) ds) := EMBED(R)
  # Build models
  lmMdl <- lm(label ~ ., data = ds) # same as glm with Gaussian family
  glmMdl <- glm(label ~ ., family = binomial, data = ds)
  
  # Get model predictions
  dsPreds <- ds['label']
  dsPreds$lmPreds <- predict(lmMdl, ds)
  dsPreds$glmPreds <- predict(glmMdl, ds, type = 'response')

  # Also return the goodness of fit information
  dsFit <- c(
    capture.output(summary(lmMdl)),
    "===================================================",
    capture.output(summary(glmMdl))
  )
  fit<-data.frame(dsFit, stringsAsFactors = F)

  # This code generates different results after the 13th decimal digit
  # on Ubuntu 16.04 and CentOS
  # To be able to use environment independent key file to check the result of this test,
  # we round the predicted values to 11 decimal digits.

  dsPreds$lmPreds <- round(dsPreds$lmPreds, digits=11)
  dsPreds$glmPreds <- round(dsPreds$glmPreds, digits=11)

  # Output to HPCC
  list(predictions=dsPreds, fit=fit)
ENDEMBED;

result := runAnalyses(irisData);
result.predictions;
result.fit;

