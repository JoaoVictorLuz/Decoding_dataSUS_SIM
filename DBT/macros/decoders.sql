{% macro decode_sexo(col) %}
    case {{col}}
        when '1' then 'masculino'
        when '2' then 'feminino'
        else 'ignorado'
    end
{% endmacro %}

{% macro decode_racacor(col) %}
    case trim({{col}})
        when '-' then 'ignorado'
        when '1' then 'branca'
        when '2' then 'preta'
        when '3' then 'amarela'
        when '4' then 'parda'
        when '5' then 'indigena'
    end
{% endmacro %}


{% macro decode_tipobito(col) %}
    case trim({{col}})
        when '1' then 'fetal'
        when '2' then 'nao_fetal'
    end
{% endmacro %}

{% macro decode_esc(col) %}
    case trim({{col}})
        when '1' then 'nenhuma'
        when '2' then '1 a 3 anos'
        when '3' then '4 a 7 anos'
        when '4' then '8 a 11 anos'
        when '8' then '8 a 11 anos'
        when '5' then '12 anos e mais'
        else 'ignorado'      
    end
{% endmacro %}

{% macro decode_dt(col) %}
    case 
        when length({{col}}) = 8 then TRY_TO_DATE({{col}}, 'DDMMYYYY')
        when length({{col}}) = 6 then TRY_TO_DATE({{col}}, 'MMYYYY')
        when length({{col}}) = 4 then TRY_TO_DATE({{col}}, 'YYYY')
        else null
    end
{% endmacro %}

{% macro decode_estciv(col) %}
    case trim({{col}})
        when '1' then 'solteiro'
        when '2' then 'casado'
        when '3' then 'viúvo'
        when '4' then 'separado'
        when '5' then 'união estável'
        when '9' then 'ignorado'
        else null
    end 
{% endmacro %}

{% macro decode_local_do_obito(col) %}
    case trim({{col}})
        when '1' then 'hospital'
        when '2' then 'outros estabelecimentos de saúde'
        when '3' then 'domicílio'
        when '4' then 'via pública'
        when '5' then 'outros'
        when '6' then 'aldeia indígena'
        when '9' then 'ignorado'
        else null
    end
{% endmacro %}

{% macro decode_gravidez(col) %}
    case trim({{col}})
        when '1' then 'única'
        when '2' then 'dupla'
        when '3' then 'tripla'
        when '9' then 'ignorada'
        else null
    end
{% endmacro %}

{% macro decode_tipo_de_parto(col) %}
    case trim({{col}})
        when '1' then 'vaginal'
        when '2' then 'cesáreo'
        when '9' then 'ignorado'
        else null
    end
{% endmacro %}

{% macro decode_obitoparto(col) %}
    case trim({{col}})
        when '1' then 'antes'
        when '2' then 'durante'
        when '3' then 'depois'
        when '9' then 'ignorado'
        else null
    end
{% endmacro %}

----- serve para obitograv, assistimed, fez_exame, fez_cirurgia, necropsia, acidtrab
{% macro decode_sim_ou_nao(col) %}
    case trim({{col}})
        when '1' then 'sim'
        when '2' then 'não'
        when '9' then 'ignorado'
        else null
    end
{% endmacro %}

{% macro decode_obitopuerp(col) %}
    case trim({{col}})
        when '1' then 'sim, até 42 dias'
        when '2' then 'sim, de 43 dias a 1 ano'
        when '3' then 'não'
        when '9' then 'ignorado'
        else null
    end
{% endmacro %}

{% macro decode_tipo_morte_nao_natural(col) %}
    case trim({{col}})
        when '1' then 'acidente'
        when '2' then 'suicídio'
        when '3' then 'homicídio'
        when '4' then 'outros'
        when '9' then 'ignorado'
        else null
    end
{% endmacro %}

{% macro decode_fonte_da_informacao(col) %}
    case trim({{col}})
        when '1' then 'ocorrência policial'
        when '2' then 'hospital'
        when '3' then 'família'
        when '4' then 'outra'
        when '9' then 'ignorado'
        else null
    end
{% endmacro %}

{% macro decode_number_clean(col) %}
    case
        when trim({{col}}) in ('A', '0 00', '4+') then null
        when trim({{col}}) = '' then null
        else try_cast(trim({{col}}) as number(5,0))
    end
{% endmacro %}








