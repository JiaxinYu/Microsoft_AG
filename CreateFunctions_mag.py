#!/usr/bin/env python3
# jiaxin_yu[v-jiaxyu@microsoft.com]

def Affiliations(rootpath):
    Affiliations = spark.read.load(rootpath + "Affiliations.txt", format="csv", sep="\t") \
    .toDF("AffiliationId", "Rank", "NormalizedName", "DisplayName", "GridId", "OfficialPage", "WikiPage", "PaperCount", "CitationCount", 
      "CreatedDate")
    return Affiliations


def Authors(rootpath): 
    Authors = spark.read.load(rootpath + "Authors.txt", format="csv", sep="\t") \
    .toDF("AuthorId", "Rank", "NormalizedName", "DisplayName", "LastKnownAffiliationId", "PaperCount", "CitationCount", "CreatedDate")
    return Authors


def ConferenceSeries(rootpath):
    ConferenceSeries = spark.read.load(rootpath + "ConferenceSeries.txt", format="csv", sep="\t") \
    .toDF("ConferenceSeriesId", "Rank", "NormalizedName", "DisplayName", "PaperCount", "CitationCount", "CreatedDate")
    return ConferenceSeries


def FieldsOfStudy(rootpath): 
    FieldsOfStudy = spark.read.load(rootpath + "FieldsOfStudy.txt", format="csv", sep="\t") \
    .toDF("FieldOfStudyId", "Rank", "NormalizedName", "DisplayName", "MainType", "Level", "PaperCount", "CitationCount", "CreatedDate")
    return FieldsOfStudy


def Journals(rootpath): 
    Journals = spark.read.load(rootpath + "Journals.txt", format="csv", sep="\t") \
    .toDF("JournalId", "Rank", "NormalizedName", "DisplayName", "Issn", "Publisher", "Webpage", "PaperCount", "CitationCount", "CreatedDate")
    return Journals


def Papers(rootpath): 
    Papers = spark.read.load(rootpath + "Papers.txt", format="csv", sep="\t") \
    .toDF("PaperId", "Rank", "Doi", "DocType", "PaperTitle", "OriginalTitle", "BookTitle", "Year", "Date", "Publisher", "JournalId", 
          "ConferenceSeriesId", "ConferenceInstanceId", "Volume", "Issue", "FirstPage", "LastPage", "ReferenceCount", "CitationCount", 
          "EstimatedCitationCount", "CreatedDate")
    return Papers


def PaperAuthorAffiliations(rootpath):
    PaperAuthorAffiliations = spark.read.load(rootpath + "PaperAuthorAffiliations.txt", format="csv", sep="\t") \
    .toDF("PaperId", "AuthorId", "AffiliationId", "AuthorSequenceNumber", "OriginalAffiliation")
    return PaperAuthorAffiliations


def PaperFieldsOfStudy(rootpath): 
    PaperFieldsOfStudy = spark.read.load(rootpath + "PaperFieldsOfStudy.txt", format="csv", sep="\t") \
    .toDF("PaperId", "FieldOfStudyId", "Score")
    return PaperFieldsOfStudy


def PaperReferences(rootpath):
    PaperReferences = spark.read.load(rootpath + "PaperReferences.txt", format="csv", sep="\t") \
    .toDF("PaperId", "PaperReferenceId")
    return PaperReferences


# .toDF("FieldOfStudyId1", "DisplayName1", "Type1", "FieldOfStudyId2", "DisplayName2", "Type2", "Rank")
