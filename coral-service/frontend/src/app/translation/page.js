'use client';

import { useState } from 'react';

import NavBar from '@/app/components/NavBar';
import TranslationForm from '@/app/components/Forms/TranslationForm';
import SourceQueryCard from '@/app/components/Cards/SourceQueryCard';
import TranslatedQueryCard from '@/app/components/Cards/TranslatedQueryCard';
import CoralIRTextCard from '@/app/components/Cards/CoralIRTextCard';
import GraphCard from '@/app/components/Cards/GraphCard';
import Image from 'next/image';

export default function TranslationPage() {
  const [translationResult, setTranslationResult] = useState(null);
  const [translationError, setTranslationError] = useState(null);
  const [imageIDs, setImageIDs] = useState(null);
  const [imageFetchError, setImageFetchError] = useState(null);
  const [coralIRData, setCoralIRData] = useState(null);
  const [coralIRError, setCoralIRError] = useState(null);

  const handleTranslationFetch = (result, error) => {
    setTranslationResult(result);
    setTranslationError(error);
  };

  const handleImageFetch = (graphs, error) => {
    setImageIDs(graphs);
    setImageFetchError(error);
  };

  const handleCoralIRFetch = (data, error) => {
    setCoralIRData(data);
    setCoralIRError(error);
  };

  return (
    <>
      <NavBar />

      <Image
        width='200'
        height='200'
        className='mx-auto pt-4'
        src='/coral-logo.jpg'
        alt='Coral Logo'
      />

      <TranslationForm
        onTranslationFetchComplete={handleTranslationFetch}
        onImageIDsFetchComplete={handleImageFetch}
        onCoralIRFetchComplete={handleCoralIRFetch}
      />

      {translationError && (
        <div className='bg-red-50 p-6 border-2 border-red-200 rounded-3xl w-8/12 mx-auto my-3 overflow-auto sm:w-10/12'>
          <h2 className='text-xl font-bold mb-2 text-red-800'>Error</h2>
          <p className='font-courier text-red-600'>{translationError}</p>
        </div>
      )}

      {translationResult && (
        <>
          <SourceQueryCard
            query={translationResult.originalQuery}
            sourceLanguage={translationResult.sourceLanguage}
          />
          <TranslatedQueryCard
            query={translationResult.translatedQuery}
            targetLanguage={translationResult.targetLanguage}
          />
        </>
      )}

      {(coralIRData || coralIRError) && (
        <CoralIRTextCard
          relNodeText={coralIRData?.relNodeText}
          error={coralIRError}
        />
      )}

      {(imageIDs || imageFetchError) && (
        <GraphCard imageIDs={imageIDs} imageFetchError={imageFetchError} />
      )}
    </>
  );
}
