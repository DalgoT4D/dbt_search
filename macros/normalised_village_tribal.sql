{% macro normalised_village_tribal(column_name) %}
    case
        when {{ column_name }} is null then null
        else
            -- Step 3: map known spelling/suffix variants to canonical names
            -- (applied after cleaning so comparisons are clean)
            case
                regexp_replace(
                    trim(trailing '. ' from trim({{ column_name }})),
                    '\s+', ' '
                )

                -- ── Karwafa area ───────────────────────────────────────
                when 'हणपायली'          then 'हनपायली'         -- spelling variant

                -- ── Pendhari area ──────────────────────────────────────
                when 'सावंगा खुर्द.'    then 'सावंगा खुर्द'    -- trailing dot (belt-and-braces)
                when 'सावंगा बुज.'      then 'सावंगा बुज'      -- trailing dot
                when 'सावंगा बूज.'      then 'सावंगा बुज'      -- ू → ु + trailing dot
                when 'सावंगा बूज'       then 'सावंगा बुज'      -- ू → ु

                -- ── Dhanora area ───────────────────────────────────────
                when 'रेखातोला'         then 'रेखाटोला'        -- spelling variant
                when 'पन्‍नेमारा'       then 'पन्नेमारा'       -- ZWJ / narrow no-break space variant
                when 'हेट्टी'           then 'हेटी'            -- doubled consonant

                -- ── Rangi area ─────────────────────────────────────────
                -- (येनगाव 1 / येनगाव 2 are intentionally different villages; keep as-is)

                -- ── Murumgao area ──────────────────────────────────────
                when 'मुंजालगोंदि'      then 'मुंजालगोंदी'     -- इ → ई
                when 'मंगेवाडा 1'       then 'मंगेवाडा'        -- suffix: same village, keep canonical
                when 'महावाडा 2'        then 'महावाडा'         -- suffix: same village, keep canonical

                -- ── Default: return the cleaned value ──────────────────
                else
                    regexp_replace(
                        trim(trailing '. ' from trim({{ column_name }})),
                        '\s+', ' '
                    )
            end
    end
{% endmacro %}