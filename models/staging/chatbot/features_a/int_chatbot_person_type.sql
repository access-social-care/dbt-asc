{{ config(materialized='table', tags=['chatbot'], enabled=(target.type == 'duckdb')) }}

/*
  Port of ascFuncs chatbot_person_type(). Answer after the Contentful question
  "ZZZ - equalities person" (3Q896ffuUkFEi4OJa516Ag).

  R BUG, MIRRORED BY DEFAULT: R sorts events by created_at DESCENDING before
  taking lag(), so its "previous" row is the NEXT event in time. The selected
  text is therefore the user's answer to the question that follows, not the
  answer to the person-type question (in ACCESSAVA 249 of 255 values are the
  bot message "Thanks! Remember you can skip any questions...", 6 are "Next").
  Default output equals ACCESSAVA exactly. Set
    vars: {chatbot_person_type_fix_order: true}
  to take the event after the question in id order instead (232 transcripts;
  A family member of someone who needs social care / A person who needs social
  care / A social care professional / Other). See docs/parity/chatbot_features_a.md.

  Then, if the user backed up and chose again, the first row in that order is
  kept (R: slice(1)): the latest in time for the mirrored rule, the latest by
  event id for the fixed rule.
*/

{% set fix_order = var('chatbot_person_type_fix_order', false) %}

select transcript_id, person_type
from (
    select
        transcript_id,
        {{ chatbot_str_trim('text') }} as person_type,
        row_number() over (
            partition by transcript_id
            order by {{ 'id desc' if fix_order else 'created_at desc, id' }}
        ) as rn
    from ({{ chatbot_answers_after("'3Q896ffuUkFEi4OJa516Ag'", order_by=('id' if fix_order else 'created_at desc, id')) }})
    where text <> 'Skip this question' and text <> 'back'
)
where rn = 1
