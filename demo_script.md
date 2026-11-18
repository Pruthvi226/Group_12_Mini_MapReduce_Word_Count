# Demo video script — target 4 minutes 45 seconds

Replace bracketed fields with actual details. Rehearse once with the dashboard
and diagram open. Keep the recording below five minutes, including transitions;
cut pauses if needed. Save the actual recording as `demo_video.mp4` in the root.

## 0:00–0:20 — Group introduction

“Hello, we are Group 12. Our members are Nilkamal Dhruw, Nishika, Onnkareshwar,
Palak, and Panduga Pruthvi Raj. Our project is Mini
MapReduce Word Count Demo. We use Python and Streamlit to show a local simulation
of distributed processing.”

Show the title and group information.

## 0:20–1:20 — Project overview

“MapReduce breaks a data-processing problem into smaller tasks. Map processes
pieces independently. Shuffle brings results for the same key together. Reduce
combines them into a final answer. Here our keys are words, and our values are
their counts.

This approach is useful when a large job can be divided into independent pieces.
Our app makes those stages visible. It lets us enter text, upload a text file, or
load a sample. It shows chunks, mapper dictionaries, grouped counts, and the final
table. We also compare sequential mapping with Python worker processes.

All of this runs on one computer. It demonstrates the processing pattern; it is
not a network-distributed cluster.”

## 1:20–2:20 — Architecture explanation

Show `architecture.png` and trace its arrows.

“Input enters the text splitter. We choose how many chunks to create. The splitter
balances tokens without cutting them in half or losing them. If there are fewer
tokens than requested chunks, we create fewer chunks.

Each mapper lowercases words, treats punctuation as separators, and returns a
local frequency dictionary. A process pool can handle these tasks independently.
Our example has three chunks, but a chunk does not require its own dedicated
process.

Shuffle collects counts with the same word. For apple, this example collects
one, one, and one. Reduce adds the list to obtain three. Finally, Streamlit shows
the word counts, total words, unique words, and top-ten chart.”

## 2:20–4:20 — Live demo

Show a terminal with the virtual environment active. Run `streamlit run app.py`;
switch to the local browser. If startup takes time, trim that pause in editing.

“We start the dashboard with this command. First, I enter: apple banana apple
orange banana apple. The same text is also available through Load sample text.
I select three chunks and click Run MapReduce.”

Expand each mapper section.

“Chunk one is apple banana. Chunk two is apple orange. Chunk three is banana
apple. Each mapper counts the words only in its own chunk.”

Point to the grouped table.

“Shuffle groups apple as one, one, one; banana as one, one; and orange as one.
The reducer sums those values.”

Point to the final table, summary, and chart.

“We get apple three, banana two, and orange one. There are six words and three
unique words. Apple is most frequent. The chart shows these same counts.”

Point to the execution table; read the actual mode and observed times.

“Both mapping passes produce identical results. Sequential time measures a normal
loop. Parallel time includes starting and stopping the worker processes and
transferring data. For this tiny input, multiprocessing may be slower. We do not
claim it is always faster. If process execution is unavailable, the app labels a
sequential fallback.”

If time permits, show mixed case/punctuation, request more chunks than words,
or clear the input and show the empty-input message. Do not rush the main example.

## 4:20–4:45 — Learning outcomes

“We learned functional decomposition: splitter, map, shuffle, and reduce each
have one clear role. We learned independent processing and aggregation, and
verified the results with unit tests. Python multiprocessing models local worker
execution, while real distributed systems also need networking and fault
handling across machines. Thank you.”

Stop by 4:45, leaving a 15-second margin before the five-minute limit.
