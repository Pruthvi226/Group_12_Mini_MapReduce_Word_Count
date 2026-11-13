"""Run with: streamlit run app.py"""

from pathlib import Path

import streamlit as st

from mapreduce import reduce_counts, run_mappers, shuffle_results, split_text, tokenize


SAMPLE = Path(__file__).with_name("sample_input.txt").read_text(encoding="utf-8")
MAX_UPLOAD_BYTES = 5 * 1024 * 1024


def replace_input(value: str) -> None:
    st.session_state.input_text = value
    st.session_state.pop("result", None)
    st.session_state.pop("upload_error", None)
    st.session_state.upload_version += 1


def load_upload(key: str) -> None:
    uploaded = st.session_state.get(key)
    st.session_state.pop("upload_error", None)
    if uploaded is None:
        return
    try:
        if Path(uploaded.name).suffix.lower() != ".txt":
            raise ValueError("Please upload a .txt file.")
        if uploaded.size > MAX_UPLOAD_BYTES:
            raise ValueError("Please use a text file smaller than 5 MiB.")
        text = uploaded.getvalue().decode("utf-8-sig")
        if "\x00" in text:
            raise ValueError("This file appears to contain binary data.")
        st.session_state.input_text = text
        st.session_state.pop("result", None)
    except (UnicodeDecodeError, ValueError) as error:
        st.session_state.upload_error = (
            "Could not load file. Save it as UTF-8 plain text. " + str(error)
        )


def main() -> None:
    st.set_page_config(page_title="Mini MapReduce Word Count", page_icon="📊", layout="wide")
    st.title("Mini MapReduce Word Count")
    st.caption("Distributed Systems – Local Simulation")
    st.info("This is a local multiprocessing simulation, not a real network-distributed cluster.")
    st.session_state.setdefault("input_text", "")
    st.session_state.setdefault("upload_version", 0)

    st.header("Input")
    sample_col, clear_col = st.columns(2)
    sample_col.button("Load sample text", on_click=replace_input, args=(SAMPLE,))
    clear_col.button("Clear input", on_click=replace_input, args=("",))
    upload_key = f"upload_{st.session_state.upload_version}"
    st.file_uploader("Upload a UTF-8 .txt file (maximum 5 MiB)", type=["txt"],
                     key=upload_key, on_change=load_upload, args=(upload_key,))
    if "upload_error" in st.session_state:
        st.error(st.session_state.upload_error)
    text = st.text_area("Enter or edit text", key="input_text", height=180)
    chunks_requested = st.slider("Number of chunks", 1, 32, 3)
    st.caption("Chunks preserve whitespace-delimited tokens. Punctuation separates words during mapping; "
               "letters and numbers are counted in lowercase. For example: can't → can, t.")

    st.header("Processing pipeline")
    st.markdown("**Input Text → Chunking → Mapping → Shuffle & Group → Reducing → Final Output**")
    if st.button("Run MapReduce", type="primary"):
        st.session_state.pop("result", None)
        if not text.strip():
            st.error("Enter text, upload a file, or load the sample before running.")
        elif not tokenize(text):
            st.warning("No words found. Enter at least one letter or number.")
        else:
            with st.spinner("Running sequential and parallel mapper passes…"):
                chunks = split_text(text, chunks_requested)
                sequential = run_mappers(chunks)
                parallel = run_mappers(chunks, parallel=True)
                if sequential.outputs != parallel.outputs:
                    st.error("Mapper results differed. No final result was saved.")
                else:
                    grouped = shuffle_results(parallel.outputs)
                    st.session_state.result = {
                        "input": text, "requested": chunks_requested, "chunks": chunks,
                        "sequential": sequential, "parallel": parallel, "grouped": grouped,
                        "counts": reduce_counts(grouped),
                    }

    result = st.session_state.get("result")
    if not result:
        return
    if result["input"] != text or result["requested"] != chunks_requested:
        st.warning("Input or chunk selection changed. Run MapReduce again to update the results.")
        return
    chunks = result["chunks"]
    st.header("Intermediate results")
    st.caption(f"Requested {chunks_requested} chunks; created {len(chunks)} nonempty chunks.")
    for index, (chunk, output) in enumerate(zip(chunks, result["parallel"].outputs), 1):
        with st.expander(f"Chunk {index} and mapper {index} — {sum(output.values())} words"):
            st.text(chunk)
            st.json(output)
    with st.expander("Shuffle & group", expanded=True):
        st.dataframe([{"Word": word, "Mapper counts": str(values)}
                      for word, values in result["grouped"].items()],
                     hide_index=True, width="stretch")

    st.header("Final results")
    ranked = sorted(result["counts"].items(), key=lambda item: (-item[1], item[0]))
    total_col, unique_col, frequent_col = st.columns(3)
    total_col.metric("Total words", sum(result["counts"].values()))
    unique_col.metric("Unique words", len(ranked))
    frequent_col.metric("Most frequent word", ranked[0][0], f"{ranked[0][1]} occurrences")
    st.caption("Ties are ordered alphabetically.")
    st.dataframe([{"Word": word, "Count": count} for word, count in ranked],
                 hide_index=True, width="stretch")
    st.subheader("Top 10 words")
    st.bar_chart({"Word": [word for word, _ in ranked[:10]],
                  "Count": [count for _, count in ranked[:10]]}, x="Word", y="Count", sort=False)
    seq, par = result["sequential"], result["parallel"]
    st.subheader("Mapper execution comparison")
    st.dataframe([
        {"Execution": "Sequential", "Seconds": seq.seconds, "Worker processes": seq.workers},
        {"Execution": par.mode.capitalize(), "Seconds": par.seconds, "Worker processes": par.workers},
    ], hide_index=True, width="stretch")
    if par.fallback_reason:
        st.warning(f"Parallel execution was unavailable; used sequential fallback. {par.fallback_reason}")
    st.success("Sequential and parallel-attempt mapper results match.")
    st.caption("Times measure mapping only. Parallel timing includes process startup, data transfer, "
               "and shutdown; fallback timing also includes the failed attempt. These are single runs, "
               "not a benchmark. Small inputs often take longer with multiprocessing. "
               "Tasks may share a worker; one chunk does not imply one dedicated process.")


if __name__ == "__main__":
    main()
