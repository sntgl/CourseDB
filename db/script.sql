DROP SCHEMA IF EXISTS cw CASCADE;
CREATE SCHEMA cw;

CREATE TABLE cw.Category
(
    id          SERIAL PRIMARY KEY,
    name        varchar(30)  NOT NULL UNIQUE,
    description varchar(300) NOT NULL
);

CREATE TABLE cw.Card
(
    id          SERIAL PRIMARY KEY,
    category_id INT         NOT NULL,
    text        varchar(30) NOT NULL,
    FOREIGN KEY (category_id) REFERENCES cw.Category (id)
);

CREATE TABLE cw.User
(
    id              SERIAL PRIMARY KEY,
    name            varchar(100)        not null,
    login           varchar(300) unique not null,
    password        varchar(60) unique  not null,
    access_level    smallint            not null,
    token           varchar(32) unique,
    token_timestamp timestamp
);

CREATE TABLE cw.Revision
(
    id         SERIAL PRIMARY KEY,
    name       varchar(100),
    owner_id   INT,
    created_at timestamp,
    FOREIGN KEY (owner_id) REFERENCES cw.User (id)
);

CREATE TABLE cw.CategoryT
(
    id              SERIAL PRIMARY KEY,
    revision_id     INT,
    category_id     INT unique,
    new_name        varchar(30),
    new_description varchar(30),
    FOREIGN KEY (revision_id) REFERENCES cw.Revision (id)
);
ALTER TABLE cw.CategoryT
    ADD UNIQUE (revision_id, new_name);

CREATE TABLE cw.CardT
(
    id          SERIAL PRIMARY KEY,
    revision_id INT,
    category_id INT,
    card_id     INT,
    new_text    varchar(300),
    FOREIGN KEY (revision_id) REFERENCES cw.Revision (id),
    FOREIGN KEY (category_id) REFERENCES cw.CategoryT (category_id)
);


--хранение настроек (пока только версии базы)
CREATE TABLE cw.key_values
(
    key   varchar,
    value VARCHAR
);

--extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;

--функции
--авторизация
drop type if exists cw.auth_result CASCADE;
CREATE type cw.auth_result AS
(
    id           int,
    token        varchar(32),
    access_level smallint
);

CREATE or replace FUNCTION cw.auth(login_in varchar, pw_in varchar)
    RETURNS cw.auth_result
AS
$$
DECLARE
    res cw.auth_result;
begin
    IF EXISTS(SELECT * FROM cw.user WHERE login = login_in AND password = crypt(pw_in, password)) THEN
        update cw.user
        set token           = replace(gen_random_uuid()::text, '-', ''),
            token_timestamp = now()
        where login = login_in
        returning id, token::text, access_level::text into res.id, res.token, res.access_level;
    ELSE
        select '', -1, -1 into res.token, res.access_level, res.id;
    END IF;
    return res;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--регистрация, с настраиваемым access_level
CREATE or replace FUNCTION cw.register(name_in varchar, login_in varchar, pw_in varchar, access_level_in smallint)
    RETURNS int
AS
$$
DECLARE
    res int;
begin
    IF not EXISTS(SELECT * FROM cw.user WHERE login = login_in) and length(pw_in) > 8 THEN
        insert into cw.user (name, login, password, access_level)
        values (name_in, login_in, crypt(pw_in, gen_salt('bf')), access_level_in)
        returning id into res;
    ELSE
        select -1 into res;
    END IF;
    return res;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--проверка токена, возвращает либо id + уровень доступа, либо -1
drop type if exists session_result CASCADE;
CREATE type session_result AS
(
    id           int,
    access_level smallint
);

CREATE or replace FUNCTION cw.session(token_in varchar)
    RETURNS session_result
AS
$$
declare
    ret session_result;
begin
    select id, access_level
    from cw.user
    where token = token_in
      and EXTRACT(EPOCH FROM now() - token_timestamp) < 86400
    into ret.id, ret.access_level;
    if ret.id is null then
        select -1, -1 into ret.id, ret.access_level;
    end if;
    return ret;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--получение версии
CREATE or replace FUNCTION cw.v()
    RETURNS varchar
AS
$$
BEGIN
    RETURN value from cw.key_values where key = 'v';
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--изменение категории или ее удаление из базы
CREATE or replace FUNCTION cw.edit_category(
    revision_id_in int,
    category_id_in int,
    name varchar default null,
    description varchar default null
)
    RETURNS int
AS
$$
declare
    res int;
begin
    if (exists(select *
               from cw.categoryt
               where revision_id = revision_id_in
                 and new_name = name
                 and not category_id = category_id_in)) then
        return -1;
    end if;
    if (not exists(select * from cw.category where id = category_id_in) and (name is null or description is null)) then
        return -2;
    end if;
    if (exists(select * from cw.category where id = category_id_in) and
        not exists(select * from cw.categoryt where category_id = category_id_in)) then
        insert into cw.categoryt (revision_id, category_id, new_name, new_description)
        values (revision_id_in, category_id_in, name, description)
        returning id into res;
    else
        update cw.categoryT
        set revision_id     = revision_id_in,
            category_id     = category_id_in,
            new_name        = name,
            new_description = description
        where category_id = category_id_in
        returning id into res;
    end if;
    return res;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--добавление категории
CREATE or replace FUNCTION cw.add_category(
    revision_id_in int,
    name varchar,
    description varchar
)
    RETURNS int
AS
$$
declare
    res int;
begin
    if (exists(select * from cw.categoryt where revision_id = revision_id_in and new_name = name)) then
        return -1;
    end if;
    insert into cw.categoryT(revision_id, category_id, new_name, new_description)
    values (revision_id_in, nextval('cw.category_id_seq'), name, description)
    returning category_id into res;
    return res;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--удаление изменения категории
CREATE or replace FUNCTION cw.remove_category_edition(
    revision_id_in int,
    category_id_in int
) RETURNS bool AS
$$
begin
    if (not exists(select * from cw.categoryt where revision_id = revision_id_in and category_id = category_id_in)) then
        return false;
    end if;
    delete from cw.cardt where revision_id = revision_id_in and category_id = category_id_in;
    delete from cw.categoryt where revision_id = revision_id_in and category_id = category_id_in;
    return true;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;


--проверка принадлоежности ревизии пользователю
CREATE or replace FUNCTION cw.owns(
    user_id int,
    revision_id_in int
)
    RETURNS bool
AS
$$
begin
    return exists(select * from cw.revision r where r.owner_id = user_id and r.id = revision_id_in);
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--добавление карточки в категорию
CREATE or replace FUNCTION cw.add_card(
    revision_id_in int,
    category_id_in int,
    text_in varchar
)
    RETURNS int
AS
$$
declare
    res int;
begin
    --категории не существует
    if (not exists(select * from cw.categoryt where category_id = category_id_in and revision_id = revision_id_in)) then
        return -1;
    end if;
    --карта с таким именем уже есть в этой категории
    if (exists(select *
               from cw.category cat
                        join cw.card crd on crd.category_id = cat.id
               where cat.id = category_id_in
                 and crd.text = text_in) or
        exists(select *
               from cw.categoryt cat
                        join cw.cardt crd on crd.category_id = cat.category_id
               where cat.category_id = category_id_in
                 and crd.new_text = text_in)) then
        return -2;
    end if;

    insert into cw.cardT(card_id, revision_id, category_id, new_text)
    values (nextval('cw.card_id_seq'), revision_id_in, category_id_in, text_in)
    returning card_id into res;
    return res;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--изменение карточки в категории или ее удаление из базы
CREATE or replace FUNCTION cw.edit_card(
    revision_id_in int,
    category_id_in int,
    card_id_in int,
    text_in varchar default null
)
    RETURNS int
AS
$$
declare
    res int;
begin
    --карта изменяется в другой ревизии
    if (exists(select *
               from cw.cardt
               where card_id = card_id_in
                 and not revision_id = revision_id_in
                 and not category_id = category_id_in)) then
        return -1;
    end if;
    --категории не существует
    if (not exists(select * from cw.categoryt where category_id = category_id_in and revision_id = revision_id_in)) then
        return -2;
    end if;

    if (exists(select * from cw.card where id = card_id_in and category_id = category_id_in) and
        not exists(select * from cw.cardt where card_id = card_id_in and category_id = category_id_in)) then
        insert into cw.cardt (card_id, revision_id, category_id, new_text)
        values (card_id_in, revision_id_in, category_id_in, text_in)
        returning card_id into res;
    else
        update cw.cardT
        set card_id     = card_id_in,
            revision_id = revision_id_in,
            category_id = category_id_in,
            new_text    = text_in
        where category_id = category_id_in
          and revision_id = revision_id_in
          and card_id = card_id_in
        returning card_id into res;
    end if;
    return res;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--удаление изменения категории
CREATE or replace FUNCTION cw.remove_card_edition(
    revision_id_in int,
    category_id_in int,
    card_id_in int
) RETURNS bool AS
$$
begin
    if (not exists(select *
                   from cw.cardt
                   where revision_id = revision_id_in
                     and category_id = category_id_in
                     and card_id = card_id_in)) then
        return false;
    end if;
    delete from cw.cardt where revision_id = revision_id_in and category_id = category_id_in and card_id = card_id_in;
    return true;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--ревизия в формате json
CREATE or replace FUNCTION cw.revision_json(revision_id_in int) RETURNS varchar as
$$
DECLARE
    res varchar;
begin
    select json_build_object(
                   'name', r.name,
                   'created_at', r.created_at,
                   'categories', j.list)
    from (select array_to_json(array_agg(json_build_object(
            'category_id', c.category_id,
            'name', c.new_name,
            'description', new_description,
            'cards', c.cards))) list
          from cw.revision_categories_with_cards c
          where c.revision_id = revision_id_in) j
             join cw.revision r on r.id = revision_id_in
    into res;
    return res;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

--удаление ревизии
CREATE or replace procedure cw.remove_revision(revision_id_in int)
    LANGUAGE plpgsql AS
$$
begin
    DELETE FROM cw.cardt where revision_id = revision_id_in;
    DELETE FROM cw.categoryt where revision_id = revision_id_in;
    DELETE FROM cw.revision where id = revision_id_in;
END
$$
    SECURITY DEFINER;


--применение ревизии
CREATE or replace procedure cw.revision_apply(revision_id_in int)
    LANGUAGE plpgsql AS
$$
begin
    --удаление всех карт для категорий из базы - с полями null
    delete
    from cw.card as crd
    where crd.category_id in (select category_id
                              from cw.categoryt
                              where revision_id = revision_id_in
                                and new_description is null
                                and new_name is null);
    --удаление всех категорий из базы - с полями null
    delete
    from cw.category as cat
    where cat.id in (select category_id
                     from cw.categoryt
                     where revision_id = revision_id_in
                       and new_description is null
                       and new_name is null);
    --удаляем карточки ревизии примененных категорий
    delete
    from cw.cardt
    where category_id in (select category_id
                          from cw.categoryt
                          where revision_id = revision_id_in
                            and new_description is null
                            and new_name is null);
    --удаляем примененные категории ревизии
    delete
    from cw.categoryt ct
    where ct.revision_id = revision_id_in
      and new_description is null
      and new_name is null;

    --добавляем недостающие данные в категории
    update cw.categoryt ct
    set new_description = coalesce(new_description, sq.d),
        new_name        = coalesce(new_name, sq.d)
    from (select id i, name n, description d from cw.category) sq
    where revision_id = revision_id_in
      and sq.i = ct.id;

    --вставляем либо обновляем категории
    with new_categories_cte as (select ct.category_id                              as id,
                                       coalesce(ct.new_name, c.name)               as name,
                                       coalesce(ct.new_description, c.description) as description
                                from (select * from cw.categoryt where revision_id = revision_id_in) ct
                                         left join cw.category c on ct.category_id = c.id)
    insert
    into cw.category as c (id, name, description)
    select n.id, n.name, n.description
    from new_categories_cte n
    on conflict (id)
        do update
        set name = excluded.name, description = excluded.description;

    --вставляем либо обновляем карточки
    with new_cards_cte as (select ct.card_id                    as id,
                                  coalesce(ct.new_text, c.text) as text,
                                  ct.category_id as cid
                           from (select * from cw.cardt where revision_id = revision_id_in) ct
                                    left join cw.card c on ct.card_id = c.id)
    insert
    into cw.card as c (id, category_id, text)
    select n.id, n.cid, n.text
    from new_cards_cte n
    on conflict (id)
        do update
        set text = excluded.text;

    call cw.remove_revision(revision_id_in);
end;
$$
    SECURITY DEFINER;

--вьюхи
--все категории с карточками для ревизии в формате json
create or replace view cw.revision_categories_with_cards as
select id,
       category_id,
       new_name,
       new_description,
       revision_id,
       (select array_to_json(array_agg(row_to_json(cards.*))) as array_to_json
        from (select c.card_id, c.new_text
              from cw.cardt c
              where c.revision_id = r.revision_id
                and c.category_id = r.category_id) cards) as cards
from cw.categoryt r;

--пользователь watcher
-- DROP OWNED BY watcher_user;
-- drop role if exists watcher_user;
create role watcher_user login password '8B137DEC7A74463EB1836CA141BEADB3';
revoke all on all tables in schema public from watcher_user;
revoke all on all tables in schema cw from watcher_user;
revoke execute on all functions in schema cw from watcher_user;
grant usage on schema public to watcher_user;
grant usage on schema cw to watcher_user;
grant select on cw.Category, cw.Card to watcher_user;
grant EXECUTE on function cw.v to watcher_user;
grant EXECUTE on function cw.auth to watcher_user;
grant EXECUTE on function cw.session to watcher_user;


--пользователь editor
-- DROP OWNED BY editor_user;
-- drop role if exists editor_user;
create role editor_user LOGIN password 'FBEC626988EE4A02949A95C8B5BB113A';
revoke all on all tables in schema public from editor_user;
revoke all on all tables in schema cw from editor_user;
revoke execute on all functions in schema cw from editor_user;
grant usage on schema public to editor_user;
grant usage on schema cw to editor_user;
grant select on cw.Category, cw.Card, cw.CardT, cw.CategoryT, cw.Revision to editor_user;
grant select on cw.revision_categories_with_cards to editor_user;
grant insert on cw.Revision to editor_user;

grant EXECUTE on function cw.v to editor_user;
grant EXECUTE on function cw.auth to editor_user;
grant EXECUTE on function cw.session to editor_user;
grant EXECUTE on function cw.edit_category to editor_user;
grant EXECUTE on function cw.add_category to editor_user;
grant EXECUTE on function cw.remove_category_edition to editor_user;
grant EXECUTE on function cw.owns to editor_user;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA cw TO editor_user;


--стартовый набор данных
--Админ с id=0 и паролем MyPass251 по SHA-1
insert into cw.key_values (key, value)
values ('v', '19A634B954C14EEB97EA542D9AF0344D');
insert into cw.category (name, description)
values ('Простые', 'Для простой игры'),
       ('Сложные', 'Для сложной игры');
insert into cw.card (category_id, text)
values (1, 'Арбуз'),
       (1, 'Совок'),
       (1, 'Колбаса');
insert into cw.card (category_id, text)
values (2, 'Автобус'),
       (2, 'Сова'),
       (2, 'Аэропорт');


select cw.register('Aleksandr Tagilov'::varchar, 'amtagilov'::varchar, 'MySimplePassword123'::varchar, 1::smallint);

--примеры использования
select a.token, a.access_level
from cw.auth('amtagilov', 'MySimplePassword123') as a;
select cw.session('dda5510f4b5844a3a3000710224a93df');
SELECT s.id, s.access_level
FROM cw.session('dda5510f4b5844a3a3000710224a93df') as s;
select v();
