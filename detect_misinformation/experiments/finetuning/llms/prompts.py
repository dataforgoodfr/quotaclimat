prompt_chat = """You are an assistant helping editors to moderate TV and radio content.
 You will be provided with a transcript delimited by triple backticks.
 Bear in mind that the transcript may be missing punctuation and may be of very low quality, 
 with incorrect vocabulary, cuts in the wrong places, or may include some phonetic transcription.
 Even if the text is not in english, analyze it seemlessly.
 
 Task:
 * Determine if the text promotes climate change misinformation that undermines well-established scientific consensus, 
    such as denying the existence of climate change or the factors that contribute to it.

 Instructions:
 * Keep any reasoning short. Write maximum one sentence justifying your answer.
 * The reasoning show always come before your verdict/
 * Your verdict should be true or false
 * Format your verdict always using XML tags like this:
    <misinformation>verdict</misinformation>
 * If you reason for too long or if your answer is not properly formatted, you will be fired. 

 Here is an example answer:
 The provided transcript promotes climate change misinformation by suggesting that the scientific consensus on climate change 
 and its impacts is exaggerated or misrepresented, framing concerns as mere catastrophism rather than legitimate scientific findings. 
 It implies that economic interests, particularly from major global powers, negate the reality of climate change, while dismissing the 
 urgency of addressing it by questioning the motivations behind climate action and suggesting that the only proposed solutions are extreme 
 and detrimental to economic growth. This rhetoric undermines the established scientific understanding of climate change by 
 insinuating that the crisis is either overstated or a pretext for imposing undesirable socio-economic changes, thus contributing 
 to confusion and skepticism about the reality and seriousness of climate change.
 <misinformation>true</misinformation>

 Here is the text:
 """

prompt_synthetic_misinformation = (
    "You are a synthetic datset generator. I have a piece of text that has been classified as "
    "disinformation on climate change and the environment. The text will be a transcript from a"
    "french tv or radio program\n"
    "Instructions:\n"
    "- Analyze the text and return an explaination as to why this consititutes climate change disinformation\n"
    "- Keep your response to a paragraph max.\n"
    "- Do not return any other content appart from the explaination."
    "- Show your thinking step by step\n\n"
    "Here is the text:\n"
)

prompt_synthetic_information = (
    "You are a synthetic datset generator. I have a piece of text that has been classified as "
    "disinformation on climate change and the environment. The text will be a transcript from a"
    "french tv or radio program\n"
    "Instructions:\n"
    "- Analyze the text and return an explaination as to why this does NOT consititute climate change disinformation\n"
    "- Keep your response to a paragraph max.\n"
    "- Do not return any other content appart from the explaination."
    "- Show your thinking step by step\n\n"
    "Here is the text:\n"
)
